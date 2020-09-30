package eval

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/grafana/grafana/pkg/bus"
	"github.com/grafana/grafana/pkg/components/simplejson"
	"github.com/grafana/grafana/pkg/models"
	"github.com/grafana/grafana/pkg/plugins"
	"github.com/grafana/grafana/pkg/registry"
	"github.com/grafana/grafana/pkg/services/datasources"
	"github.com/grafana/grafana/pkg/tsdb"
)

type minimalDashboard struct {
	Panels []struct {
		ID         int64              `json:"id"`
		Datasource string             `json:"datasource"`
		Targets    []*simplejson.Json `json:"targets"`
	} `json:"panels"`
}

type AlertNG struct {
	DatasourceCache datasources.CacheService `inject:""`
}

func init() {
	registry.RegisterService(&AlertNG{})
}

// Init initializes the AlertingService.
func (e *AlertNG) Init() error {
	return nil
}

type AlertExecCtx struct {
	AlertDefitionID int64
	SignedInUser    *models.SignedInUser

	Ctx context.Context
}

// At least Warn or Crit condition must be non-empty
type Conditions struct {
	Condition string `json:"condition"`

	QueriesAndExpressions []tsdb.Query `json:"queriesAndExpressions"`
}

type ExecutionResult struct {
	AlertDefinitionId int64

	Error error

	Results data.Frames
}

type EvalResults []EvalResult

type EvalResult struct {
	Instance data.Labels
	State    State // Enum
}

type State int

const (
	Normal State = iota
	Warning
	Critical
	Error
)

func (s State) String() string {
	return [...]string{"Normal", "Warning", "Critical", "Error"}[s]
}

func (c Conditions) isValid() bool {
	/*
		if c.WarnCondition == "" && c.CritCondition == "" {
			return false
		}
	*/

	if len(c.QueriesAndExpressions) == 0 {
		return false
	}
	// TODO search for refIDs in QueriesAndExpressions

	return true
}

// LoadAlertConditions returns a Conditions object for the given alertDefintionId.
func (ng *AlertNG) LoadAlertConditions(alertDefinitionID int64, signedInUser *models.SignedInUser, skipCache bool) (*Conditions, error) {
	getAlertByIDQuery := models.GetAlertByIdQuery{Id: alertDefinitionID}
	if err := bus.Dispatch(&getAlertByIDQuery); err != nil {
		return nil, err
	}

	// get queries from the dashboard (because GEL expressions cannot be stored in alerts so far)
	getDashboardQuery := models.GetDashboardQuery{Id: getAlertByIDQuery.Result.DashboardId}
	if err := bus.Dispatch(&getDashboardQuery); err != nil {
		return nil, err
	}

	blob, err := getDashboardQuery.Result.Data.MarshalJSON()
	if err != nil {
		return nil, errors.New("Failed to marshal dashboard JSON")
	}
	var dash minimalDashboard
	err = json.Unmarshal(blob, &dash)
	if err != nil {
		return nil, errors.New("Failed to unmasrhal dashboard JSON")
	}

	conditions := Conditions{}
	for _, p := range dash.Panels {
		if p.ID == getAlertByIDQuery.Result.PanelId {
			panelDatasource := p.Datasource
			var ds *models.DataSource
			for i, query := range p.Targets {
				refID := query.Get("refId").MustString("A")
				queryDatasource := query.Get("datasource").MustString()
				if queryDatasource == "__expr__" {
					conditions.Condition = refID // for now use the last expression for condition
				}

				if i == 0 && queryDatasource != "__expr__" {
					dsName := panelDatasource
					if queryDatasource != "" {
						dsName = queryDatasource
					}

					getDataSourceByNameQuery := models.GetDataSourceByNameQuery{Name: dsName, OrgId: getDashboardQuery.Result.OrgId}
					if err := bus.Dispatch(&getDataSourceByNameQuery); err != nil {
						return nil, err
					}

					ds, err = ng.DatasourceCache.GetDatasource(getDataSourceByNameQuery.Result.Id, signedInUser, skipCache)
					if err != nil {
						return nil, err
					}
				}

				if ds == nil {
					return nil, errors.New("No datasource reference found")
				}

				if queryDatasource == "" {
					query.Set("datasource", ds.Name)
				}

				if query.Get("datasourceId").MustString() == "" {
					query.Set("datasourceId", ds.Id)
				}

				if query.Get("orgId").MustString() == "" { // GEL requires orgID
					query.Set("orgId", 0)
				}

				if query.Get("maxDataPoints").MustString() == "" { // GEL requires orgID
					query.Set("maxDataPoints", 100)
				}

				// intervalMS is calculated by the frontend
				if query.Get("intervalMs").MustString() == "" { // GEL requires orgID
					query.Set("intervalMs", 1000)
				}

				conditions.QueriesAndExpressions = append(conditions.QueriesAndExpressions, tsdb.Query{
					RefId:         refID,
					MaxDataPoints: query.Get("maxDataPoints").MustInt64(100),
					IntervalMs:    query.Get("intervalMs").MustInt64(1000),
					QueryType:     query.Get("queryType").MustString(""),
					Model:         query,
					DataSource:    ds,
				})
				fmt.Printf(">>> query: %+v\n", conditions.QueriesAndExpressions[i])
			}
		}
	}
	blob, err = json.Marshal(conditions)
	if err != nil {
		return nil, err
	}

	return &conditions, nil
}

// Execute runs the WarnCondition and CritCondtion expressions or queries.
func (conditions *Conditions) Execute(ctx AlertExecCtx, fromStr, toStr string) (*ExecutionResult, error) {
	result := ExecutionResult{}

	request := &tsdb.TsdbQuery{
		TimeRange: tsdb.NewTimeRange(fromStr, toStr),
		Debug:     true,
		User:      ctx.SignedInUser,
	}
	for i := range conditions.QueriesAndExpressions {
		request.Queries = append(request.Queries, &conditions.QueriesAndExpressions[i])
	}

	resp, err := plugins.Transform.Transform(ctx.Ctx, request)
	if err != nil {
		result.Error = err
		return &result, err
	}
	conditionResult := resp.Results[conditions.Condition]
	result.Results, err = conditionResult.Dataframes.Decoded()
	if err != nil {
		result.Error = err
		return &result, err
	}

	return &result, nil
}

// EvaluateExecutionResult takes the ExecutionResult, and returns a frame where
// each column is a string type that holds a string representing its state.
func EvaluateExecutionResult(results *ExecutionResult) (EvalResults, error) {
	evalResults := make([]EvalResult, 0)
	for _, f := range results.Results {
		t, _ := f.StringTable(5, 5)
		fmt.Println(">>>> EvaluateExecutionResult: ", t)

		rowLen, err := f.RowLen()
		if err != nil {
			return nil, fmt.Errorf("Unable to get frame row length")
		}
		if rowLen > 1 {
			return nil, fmt.Errorf("Invalid frame %v: row length %v", f.Name, rowLen)
		}

		if len(f.Fields) > 1 {
			return nil, fmt.Errorf("Invalid frame %v: field length %v", f.Name, len(f.Fields))
		}

		if f.Fields[0].Type() != data.FieldTypeNullableFloat64 {
			return nil, fmt.Errorf("Invalid frame %v: field type %v", f.Name, f.Fields[0].Type())
		}

		state := Normal
		val, ok := f.Fields[0].ConcreteAt(0)
		if ok && val != 0 {
			state = Critical
		}

		evalResults = append(evalResults, EvalResult{
			Instance: f.Fields[0].Labels,
			State:    state,
		})
	}
	return evalResults, nil
}

// AsDataFrame forms the EvalResults in Frame suitable for displaying in the table panel of the front end.
// This may be temporary, as there might be a fair amount we want to display in the frontend, and it might not make sense to store that in data.Frame.
// For the first pass, I would expect a Frame with a single row, and a column for each instance with a boolean value.
func (evalResults EvalResults) AsDataFrame() data.Frame {
	fields := make([]*data.Field, 0)
	for _, evalResult := range evalResults {
		fields = append(fields, data.NewField("", evalResult.Instance, []bool{evalResult.State == Normal}))
	}
	f := data.NewFrame("", fields...)
	t, _ := f.StringTable(5, 5)
	fmt.Println("<<<< AsDataFrame: ", t)
	return *f
}