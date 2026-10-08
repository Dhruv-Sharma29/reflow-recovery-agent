import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from app.auth import get_api_key
from app.config import Settings, settings
from app.main import app

@pytest.fixture
def configured(monkeypatch):
    monkeypatch.setattr(settings, "api_secret_key", "")
    monkeypatch.setattr(settings, "environment", "development")
    monkeypatch.setattr(settings, "allow_unauthenticated_development", False)


def test_settings_default_to_authenticated():
    assert Settings(_env_file=None).allow_unauthenticated_development is False


def test_empty_key_refuses_access(configured):
    with pytest.raises(HTTPException) as e:
        get_api_key(None)
    assert e.value.status_code == 503
    client = TestClient(app)
    client.headers.clear()
    assert client.get("/api/dashboard/telemetry").status_code == 503


def test_explicit_development_opt_in(configured, monkeypatch):
    monkeypatch.setattr(settings, "allow_unauthenticated_development", True)
    assert get_api_key(None) is None


@pytest.mark.parametrize("environment", ["production", "staging", "Development"])
def test_opt_in_does_not_disable_production_auth(configured, monkeypatch, environment):
    monkeypatch.setattr(settings, "allow_unauthenticated_development", True)
    monkeypatch.setattr(settings, "environment", environment)
    with pytest.raises(HTTPException) as e:
        get_api_key(None)
    assert e.value.status_code == 503


def test_configured_key_is_required_even_in_development(configured, monkeypatch):
    monkeypatch.setattr(settings, "api_secret_key", "test-key")
    monkeypatch.setattr(settings, "allow_unauthenticated_development", True)
    for supplied, code in [(None, 401), ("wrong", 403), ("🔒", 403)]:
        with pytest.raises(HTTPException) as e:
            get_api_key(supplied)
        assert e.value.status_code == code
    assert get_api_key("test-key") == "test-key"
