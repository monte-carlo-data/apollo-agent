import requests

from apollo.integrations.ctp.errors import CtpPipelineError
from apollo.integrations.ctp.models import PipelineState, TransformStep
from apollo.integrations.ctp.template import TemplateEngine
from apollo.integrations.ctp.transforms.base import Transform
from apollo.integrations.ctp.transforms.registry import TransformRegistry

_V2_LOGIN_PATH = "/ma/api/v2/user/login"
_V3_LOGIN_PATH = "/saas/public/core/v3/login"
_JWT_LOGIN_PATH = "/ma/api/v2/user/loginOAuth"
_INTEGRATION_CLOUD_PRODUCT_NAME = "Integration Cloud"
_DEFAULT_BASE_URL = "https://dm-us.informaticacloud.com"
_LOGIN_TIMEOUT = 30  # seconds — matches the OAuth transform convention


def _login_failure_detail(exc: Exception) -> str:
    """Return a safe, diagnostic detail string for a login exception.

    Includes HTTP status code and Informatica error text for HTTPError (safe —
    response body never contains credentials), or the exception class name for
    other errors.
    """
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        detail = f"HTTP {exc.response.status_code}"
        try:
            body = exc.response.json()
            # Informatica error responses use "@type" and "message" or top-level "message"
            message = body.get("message") or body.get("error")
            if message:
                detail += f": {message}"
        except Exception:
            text = (exc.response.text or "")[:200].strip()
            if text:
                detail += f": {text}"
        return detail
    return type(exc).__name__


class ResolveInformaticaSessionTransform(Transform):
    """Exchange Informatica credentials for a session ID and API base URL.

    Supports two auth modes (determined by which input keys are present):

    **Password mode** (V2 or V3 username/password login):
        Requires: username, password
        Optional: informatica_auth ("v2" | "v3", default "v3"), base_url

        - "v2": POST /ma/api/v2/user/login → {serverUrl, icSessionId}
        - "v3": POST /saas/public/core/v3/login → {products[Integration Cloud].baseApiUrl,
          userInfo.sessionId}

    **JWT mode** (POST /ma/api/v2/user/loginOAuth — requires SAML configured org-wide):
        Requires: jwt_token, org_id
        Optional: base_url

        The JWT is a short-lived access token from the customer's IDP (Okta, Azure AD, etc.)
        obtained via ROPC grant. The loginOAuth response has the same shape as V2 login,
        so api_base_url and session_id are extracted identically.

    Step input keys:
        base_url        (optional): login base URL — defaults to https://dm-us.informaticacloud.com
        username        (password mode): Informatica username
        password        (password mode): Informatica password
        informatica_auth (password mode, optional): "v2" or "v3" (default "v3")
        jwt_token       (JWT mode): short-lived JWT from the customer's IDP
        org_id          (JWT mode): Informatica organization ID

    Step output keys:
        session_id:   derived key where the icSessionId string is stored
        api_base_url: derived key where the API base URL string is stored
    """

    optional_input_keys = (
        "base_url",
        "username",
        "password",
        "informatica_auth",
        "jwt_token",
        "org_id",
    )
    required_output_keys = ("session_id", "api_base_url")
    optional_output_keys = ()

    def _execute(self, step: TransformStep, state: PipelineState) -> None:
        base_url = (
            TemplateEngine.render(step.input.get("base_url", "{{ none }}"), state)
            or _DEFAULT_BASE_URL
        ).rstrip("/")

        jwt_token = TemplateEngine.render(
            step.input.get("jwt_token", "{{ none }}"), state
        )

        if jwt_token:
            api_base_url, session_id = self._login_jwt(
                base_url=base_url,
                org_id=self._require(
                    step, state, "org_id", "required with 'jwt_token'"
                ),
                jwt_token=jwt_token,
                step_name=step.type,
            )
        else:
            username = self._require(
                step,
                state,
                "username",
                "required in password mode (no 'jwt_token' provided)",
            )
            password = self._require(
                step,
                state,
                "password",
                "required in password mode (no 'jwt_token' provided)",
            )
            informatica_auth = (
                TemplateEngine.render(
                    step.input.get("informatica_auth", "{{ none }}"), state
                )
                or "v3"
            )

            if informatica_auth == "v2":
                api_base_url, session_id = self._login_v2(
                    base_url, username, password, step.type
                )
            elif informatica_auth == "v3":
                api_base_url, session_id = self._login_v3(
                    base_url, username, password, step.type
                )
            else:
                raise CtpPipelineError(
                    stage="transform_execute",
                    step_name=step.type,
                    message=(
                        f"Unsupported 'informatica_auth' value: '{informatica_auth}'. "
                        "Expected 'v2' or 'v3'."
                    ),
                )

        state.derived[step.output["session_id"]] = session_id
        state.derived[step.output["api_base_url"]] = api_base_url

    @staticmethod
    def _require(
        step: TransformStep,
        state: PipelineState,
        key: str,
        reason: str,
    ) -> str:
        value = TemplateEngine.render(step.input.get(key, "{{ none }}"), state)
        if not value:
            raise CtpPipelineError(
                stage="transform_execute",
                step_name=step.type,
                message=f"'{key}' is {reason}",
            )
        return value

    @staticmethod
    def _login_v2(
        base_url: str,
        username: str,
        password: str,
        step_name: str,
    ) -> tuple[str, str]:
        """POST /ma/api/v2/user/login → (api_base_url, session_id)."""
        try:
            response = requests.post(
                f"{base_url}{_V2_LOGIN_PATH}",
                data={"username": username, "password": password},
                timeout=_LOGIN_TIMEOUT,
            )
            response.raise_for_status()
            body = response.json()
            server_url = body.get("serverUrl")
            session_id = body.get("icSessionId")
            if not server_url or not session_id:
                raise CtpPipelineError(
                    stage="transform_execute",
                    step_name=step_name,
                    message="V2 login response missing 'serverUrl' or 'icSessionId'",
                )
            return server_url, session_id
        except CtpPipelineError:
            raise
        except Exception as exc:
            raise CtpPipelineError(
                stage="transform_execute",
                step_name=step_name,
                message=f"Informatica V2 login failed: {_login_failure_detail(exc)}",
            ) from exc

    @staticmethod
    def _login_v3(
        base_url: str,
        username: str,
        password: str,
        step_name: str,
    ) -> tuple[str, str]:
        """POST /saas/public/core/v3/login → (api_base_url, session_id)."""
        try:
            response = requests.post(
                f"{base_url}{_V3_LOGIN_PATH}",
                json={"username": username, "password": password},
                timeout=_LOGIN_TIMEOUT,
            )
            response.raise_for_status()
            body = response.json()
            products = body.get("products") or []
            api_base_url = next(
                (
                    p.get("baseApiUrl")
                    for p in products
                    if p.get("name") == _INTEGRATION_CLOUD_PRODUCT_NAME
                ),
                None,
            )
            user_info = body.get("userInfo") or {}
            session_id = user_info.get("sessionId")
            if not api_base_url or not session_id:
                raise CtpPipelineError(
                    stage="transform_execute",
                    step_name=step_name,
                    message=(
                        "V3 login response missing Integration Cloud 'baseApiUrl' "
                        "or 'userInfo.sessionId'"
                    ),
                )
            return api_base_url, session_id
        except CtpPipelineError:
            raise
        except Exception as exc:
            raise CtpPipelineError(
                stage="transform_execute",
                step_name=step_name,
                message=f"Informatica V3 login failed: {_login_failure_detail(exc)}",
            ) from exc

    @staticmethod
    def _login_jwt(
        base_url: str,
        org_id: str,
        jwt_token: str,
        step_name: str,
    ) -> tuple[str, str]:
        """POST /ma/api/v2/user/loginOAuth → (api_base_url, session_id).

        The loginOAuth response has the same {serverUrl, icSessionId} shape as V2 login,
        so extraction is identical.
        """
        try:
            response = requests.post(
                f"{base_url}{_JWT_LOGIN_PATH}",
                json={"orgId": org_id, "oauthToken": jwt_token},
                timeout=_LOGIN_TIMEOUT,
            )
            response.raise_for_status()
            body = response.json()
            server_url = body.get("serverUrl")
            session_id = body.get("icSessionId")
            if not server_url or not session_id:
                raise CtpPipelineError(
                    stage="transform_execute",
                    step_name=step_name,
                    message="JWT loginOAuth response missing 'serverUrl' or 'icSessionId'",
                )
            return server_url, session_id
        except CtpPipelineError:
            raise
        except Exception as exc:
            raise CtpPipelineError(
                stage="transform_execute",
                step_name=step_name,
                message=f"Informatica JWT login failed: {_login_failure_detail(exc)}",
            ) from exc


TransformRegistry.register(
    "resolve_informatica_session", ResolveInformaticaSessionTransform
)
