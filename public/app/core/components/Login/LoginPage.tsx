// Libraries
import React, { FC, useState } from 'react';
import { cx, keyframes, css } from 'emotion';

// Components
import { UserSignup } from './UserSignup';
import { LoginServiceButtons } from './LoginServiceButtons';
import LoginCtrl from './LoginCtrl';
import { LoginForm } from './LoginForm';
import { ChangePassword } from './ChangePassword';
import { Branding } from 'app/core/components/Branding/Branding';
import { useStyles, HorizontalGroup, Button } from '@grafana/ui';
import { GrafanaTheme } from '@grafana/data';
import { ForgottenPassword } from './ForgottenPassword';

export const LoginPage: FC = () => {
  const loginStyles = useStyles(getLoginStyles);
  const [forgotPassword, setForgotPassword] = useState(false);
  return (
    <Branding.LoginBackground className={loginStyles.container}>
      <div className={cx(loginStyles.loginContent, Branding.LoginBoxBackground())}>
        <div className={loginStyles.loginLogoWrapper}>
          <Branding.LoginLogo className={loginStyles.loginLogo} />
          <div className={loginStyles.titleWrapper}>
            <h1 className={loginStyles.mainTitle}>{Branding.LoginTitle}</h1>
            <h3 className={loginStyles.subTitle}>{Branding.GetLoginSubTitle()}</h3>
          </div>
        </div>
        <LoginCtrl>
          {({
            loginHint,
            passwordHint,
            ldapEnabled,
            authProxyEnabled,
            disableLoginForm,
            disableUserSignUp,
            login,
            isLoggingIn,
            changePassword,
            skipPasswordChange,
            isChangingPassword,
            resetCode,
          }) => (
            <div className={loginStyles.loginOuterBox}>
              {!(isChangingPassword || forgotPassword || resetCode) && (
                <div className={`${loginStyles.loginInnerBox} ${isChangingPassword ? 'hidden' : ''}`} id="login-view">
                  {!disableLoginForm && (
                    <>
                      <LoginForm
                        onSubmit={login}
                        loginHint={loginHint}
                        passwordHint={passwordHint}
                        isLoggingIn={isLoggingIn}
                      >
                        {!(ldapEnabled || authProxyEnabled) && (
                          <HorizontalGroup justify="flex-end">
                            <Button
                              className={css`
                                padding: 0;
                                margin-top: 4px;
                              `}
                              variant="link"
                              onClick={() => setForgotPassword(true)}
                            >
                              Forgot your password?
                            </Button>
                          </HorizontalGroup>
                        )}
                      </LoginForm>
                    </>
                  )}

                  <LoginServiceButtons />
                  {!disableUserSignUp && <UserSignup />}
                </div>
              )}
              {(isChangingPassword || resetCode) && (
                <div className={cx(loginStyles.loginInnerBox, loginStyles.enterAnimation)}>
                  <ChangePassword onSubmit={changePassword} onSkip={!resetCode && (skipPasswordChange as any)} />
                </div>
              )}

              {forgotPassword && (
                <div className={cx(loginStyles.loginInnerBox, loginStyles.enterAnimation)}>
                  <ForgottenPassword />
                </div>
              )}
            </div>
          )}
        </LoginCtrl>

        <div className="clearfix" />
      </div>
    </Branding.LoginBackground>
  );
};

const flyInAnimation = keyframes`
from{
  opacity: 0;
  transform: translate(-60px, 0px);
}

to{
  opacity: 1;
  transform: translate(0px, 0px);
}`;

export const getLoginStyles = (theme: GrafanaTheme) => {
  return {
    container: css`
      min-height: 100vh;
      background-position: center;
      background-repeat: no-repeat;
      min-width: 100%;
      margin-left: 0;
      background-color: $black;
      display: flex;
      align-items: center;
      justify-content: center;
    `,
    submitButton: css`
      justify-content: center;
      width: 100%;
    `,
    loginLogo: css`
      width: 100%;
      max-width: 100px;
      margin-bottom: 15px;
    `,
    loginLogoWrapper: css`
      display: flex;
      align-items: center;
      justify-content: center;
      flex-direction: column;
      padding: ${theme.spacing.lg};
    `,
    titleWrapper: css`
      text-align: center;
    `,
    mainTitle: css`
      font-size: '32px';
    `,
    subTitle: css`
      font-size: ${theme.typography.size.md};
      color: ${theme.colors.textSemiWeak};
    `,
    loginContent: css`
      max-width: 550px;
      width: 100%;
      display: flex;
      align-items: stretch;
      flex-direction: column;
      position: relative;
      justify-content: center;
      z-index: 1;
      min-height: 320px;
      border-radius: 3px;
      padding: 20px 0;
    `,
    loginOuterBox: css`
      display: flex;
      overflow-y: hidden;
      align-items: center;
      justify-content: center;
    `,
    loginInnerBox: css`
      padding: ${theme.spacing.xl};
      @media (max-width: 320px) {
        padding: ${theme.spacing.lg};
      }
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      flex-grow: 1;
      max-width: 415px;
      width: 100%;
      transform: translate(0px, 0px);
      transition: 0.25s ease;
    `,
    enterAnimation: css`
      animation: ${flyInAnimation} ease-out 0.2s;
    `,
  };
};
