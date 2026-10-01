"""Credential-safe authentication tests for MapBiomas Alerta API V2."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from scripts_python.mapbiomas_alerta_client import (  # noqa: E402
    MapbiomasAlertaClient,
    MapbiomasAlertaClientError,
)


class FakeResponse:
    def __init__(self, status_code: int, document: object) -> None:
        self.status_code = status_code
        self.document = document

    def json(self) -> object:
        return self.document


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = iter(responses)
        self.calls: list[dict] = []
        self.closed = False

    def post(self, url: str, **kwargs):
        self.calls.append({"url": url, **kwargs})
        return next(self.responses)

    def close(self) -> None:
        self.closed = True


def test_sign_in_once_and_reuse_token_without_returning_it() -> None:
    session = FakeSession(
        [
            FakeResponse(200, {"data": {"signIn": {"token": "secret-token"}}}),
            FakeResponse(200, {"data": {"alertDateRange": {"maxPublishedAt": "2026-09-24"}}}),
            FakeResponse(200, {"data": {"alertDateRange": {"maxPublishedAt": "2026-09-24"}}}),
        ]
    )
    with MapbiomasAlertaClient(email="user@example.org", password="secret-password", session=session) as client:
        assert client.query("query { alertDateRange { maxPublishedAt } }") == {
            "alertDateRange": {"maxPublishedAt": "2026-09-24"}
        }
        client.query("query { alertDateRange { maxPublishedAt } }")

    assert len(session.calls) == 3
    assert session.calls[0]["json"]["variables"] == {
        "email": "user@example.org",
        "password": "secret-password",
    }
    assert session.calls[0]["headers"] == {}
    assert session.calls[1]["headers"] == {"Authorization": "Bearer secret-token"}
    assert session.calls[2]["headers"] == {"Authorization": "Bearer secret-token"}
    assert session.closed


def test_graphql_error_does_not_expose_password_or_token() -> None:
    session = FakeSession(
        [FakeResponse(200, {"errors": [{"message": "secret-password secret-token"}]})]
    )
    client = MapbiomasAlertaClient(email="user@example.org", password="secret-password", session=session)
    with pytest.raises(MapbiomasAlertaClientError) as error:
        client.query("query { alertDateRange { maxPublishedAt } }")
    assert "secret-password" not in str(error.value)
    assert "secret-token" not in str(error.value)


def test_credentials_are_required_before_network_call() -> None:
    with pytest.raises(MapbiomasAlertaClientError, match="MAPBIOMAS_ALERTA_EMAIL"):
        MapbiomasAlertaClient(email="", password="")
