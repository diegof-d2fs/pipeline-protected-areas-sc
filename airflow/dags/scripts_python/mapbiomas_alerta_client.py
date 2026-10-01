"""Authenticated GraphQL client for MapBiomas Alerta API V2."""

from __future__ import annotations

import time
from typing import Any

import requests


DEFAULT_API_URL = "https://plataforma.alerta.mapbiomas.org/api/v2/graphql"
SIGN_IN = (
    "mutation SignIn($email: String!, $password: String!) { "
    "signIn(email: $email, password: $password) { token } }"
)


class MapbiomasAlertaClientError(RuntimeError):
    """A credential-safe API failure."""


class MapbiomasAlertaClient:
    """Sign in once per client instance and reuse its token for GraphQL queries."""

    def __init__(
        self,
        *,
        email: str,
        password: str,
        api_url: str = DEFAULT_API_URL,
        timeout_seconds: float = 60.0,
        max_attempts: int = 4,
        session: requests.Session | None = None,
    ) -> None:
        if not email or not password:
            raise MapbiomasAlertaClientError(
                "MAPBIOMAS_ALERTA_EMAIL and MAPBIOMAS_ALERTA_PASSWORD are required."
            )
        if not api_url.startswith("https://"):
            raise MapbiomasAlertaClientError("MapBiomas Alerta API URL must use HTTPS.")
        if timeout_seconds <= 0:
            raise ValueError("MapBiomas Alerta timeout must be positive.")
        if not 1 <= max_attempts <= 8:
            raise ValueError("MapBiomas Alerta max_attempts must be between one and eight.")
        self._email = email
        self._password = password
        self._api_url = api_url
        self._timeout = timeout_seconds
        self._max_attempts = max_attempts
        self._session = session or requests.Session()
        self._token: str | None = None

    def __enter__(self) -> MapbiomasAlertaClient:
        return self

    def __exit__(self, *_exc: object) -> None:
        self._session.close()

    def query(self, document: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        """Authenticate lazily; keep the token only in this process, never in XCom."""
        if self._token is None:
            self._sign_in()
        return self._post(
            {"query": document, "variables": variables or {}},
            bearer_token=self._token,
        )

    def _sign_in(self) -> None:
        data = self._post(
            {
                "query": SIGN_IN,
                "variables": {"email": self._email, "password": self._password},
            },
            bearer_token=None,
        )
        token = (data.get("signIn") or {}).get("token")
        if not isinstance(token, str) or not token:
            raise MapbiomasAlertaClientError("MapBiomas Alerta signIn returned no token.")
        self._token = token

    def _post(self, payload: dict[str, Any], bearer_token: str | None) -> dict[str, Any]:
        headers = {"Authorization": f"Bearer {bearer_token}"} if bearer_token else {}
        response = None
        for attempt in range(self._max_attempts):
            try:
                response = self._session.post(
                    self._api_url,
                    json=payload,
                    headers=headers,
                    timeout=self._timeout,
                )
            except requests.RequestException:
                if attempt + 1 == self._max_attempts:
                    raise MapbiomasAlertaClientError(
                        "MapBiomas Alerta request failed after bounded retries."
                    ) from None
            else:
                if response.status_code not in (429, 500, 502, 503, 504):
                    break
                if attempt + 1 == self._max_attempts:
                    break
            time.sleep(min(2**attempt, 8))
        if response is None:
            raise MapbiomasAlertaClientError("MapBiomas Alerta returned no response.")
        if response.status_code >= 400:
            raise MapbiomasAlertaClientError(
                f"MapBiomas Alerta returned HTTP {response.status_code}."
            )
        try:
            document = response.json()
        except ValueError:
            raise MapbiomasAlertaClientError("MapBiomas Alerta returned invalid JSON.") from None
        if not isinstance(document, dict) or document.get("errors"):
            raise MapbiomasAlertaClientError("MapBiomas Alerta returned GraphQL errors.")
        data = document.get("data")
        if not isinstance(data, dict):
            raise MapbiomasAlertaClientError("MapBiomas Alerta returned no GraphQL data.")
        return data
