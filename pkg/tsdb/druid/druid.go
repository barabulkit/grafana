package druid

import (
	"context"
	"fmt"
	"path"
	"strings"

	"golang.org/x/net/context/ctxhttp"

	"encoding/json"
	"net/http"
	"net/url"

	"sync"

	"github.com/grafana/grafana/pkg/components/simplejson"
	"github.com/grafana/grafana/pkg/models"
	"github.com/grafana/grafana/pkg/tsdb"
)

type DruidExecutor struct {
	*models.DataSource
	httpClient     *http.Client
	QueryParser    *DruidQueryParser
	ResponseParser *DruidResponseParser
	mux            sync.Mutex
}

func NewDruidExecutor(datasource *models.DataSource) (tsdb.TsdbQueryEndpoint, error) {
	httpClient, err := datasource.GetHttpClient()

	if err != nil {
		return nil, err
	}

	return &DruidExecutor{
		DataSource:     datasource,
		httpClient:     httpClient,
		QueryParser:    &DruidQueryParser{},
		ResponseParser: &DruidResponseParser{},
	}, nil
}

func init() {
	tsdb.RegisterTsdbQueryEndpoint("abhisant-druid-datasource", NewDruidExecutor)
}

func (e *DruidExecutor) getQuery(dsInfo *models.DataSource, queries []*tsdb.Query, context *tsdb.TsdbQuery) (*simplejson.Json, error) {
	if len(queries) == 0 {
		return nil, fmt.Errorf("почтиquery request contains no queries")
	}

	queryModel := queries[0].Model
	return queryModel, nil
}

func (e *DruidExecutor) Query(ctx context.Context, dsInfo *models.DataSource, queryContext *tsdb.TsdbQuery) (*tsdb.Response, error) {
	result := &tsdb.Response{}

	queryModel, err := e.getQuery(dsInfo, queryContext.Queries, queryContext)
	//queryModel := queries[0].Model

	req, err := e.createRequest(queryModel, queryContext)
	if err != nil {
		return nil, err
	}

	res, err := ctxhttp.Do(ctx, e.httpClient, req)
	if err != nil {
		return nil, err
	}

	queryResult, err := e.ResponseParser.ParseResponse(res, queryModel)
	if err != nil {
		return nil, err
	}

	result.Results = queryResult
	return result, nil
}

func (e *DruidExecutor) createRequest(data *simplejson.Json, queryContext *tsdb.TsdbQuery) (*http.Request, error) {
	u, _ := url.Parse(e.DataSource.Url)
	u.Path = path.Join(u.Path, "/druid/v2")

	// Preprocessing data to match Druid syntax
	e.QueryParser.ParseQuery(data, queryContext)

	postData, err := json.Marshal(data)

	req, err := http.NewRequest(http.MethodPost, u.String(), strings.NewReader(string(postData)))

	if err != nil {
		return nil, fmt.Errorf("Failed to create request. error: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if e.BasicAuth {
		req.SetBasicAuth(e.BasicAuthUser, e.BasicAuthPassword)
	}

	return req, err
}
