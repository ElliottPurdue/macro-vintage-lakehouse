import pytest

from macro_lake.settings import fred_api_key, load_lake_settings

ENVIRONMENT = {
    "LAKE_S3_ENDPOINT": "127.0.0.1:8333",
    "LAKE_S3_BUCKET": "lake",
    "LAKE_S3_ACCESS_KEY_ID": "an-access-key",
    "LAKE_S3_SECRET_ACCESS_KEY": "a-very-secret-value",
    "LAKE_S3_REGION": "us-east-1",
    "LAKE_S3_USE_SSL": "false",
    "LAKE_S3_URL_STYLE": "path",
}


@pytest.fixture
def environment(monkeypatch):
    """Set the lake variables and keep the developer's own .env out of the test."""
    monkeypatch.setattr("macro_lake.settings.load_dotenv", lambda *args, **kwargs: False)

    def set(**overrides):
        for name in list(ENVIRONMENT) + ["FRED_API_KEY"]:
            monkeypatch.delenv(name, raising=False)
        for name, value in {**ENVIRONMENT, **overrides}.items():
            if value is not None:
                monkeypatch.setenv(name, value)

    set()
    return set


def test_settings_come_from_the_environment(environment):
    settings = load_lake_settings()
    assert settings.endpoint_url == "http://127.0.0.1:8333"
    assert settings.uri("bronze/alfred/observations") == "s3://lake/bronze/alfred/observations"
    assert settings.use_ssl is False


def test_https_endpoint_when_ssl_is_on(environment):
    environment(LAKE_S3_USE_SSL="true")
    settings = load_lake_settings()
    assert settings.use_ssl is True
    assert settings.endpoint_url == "https://127.0.0.1:8333"


def test_a_missing_setting_names_itself(environment):
    environment(LAKE_S3_BUCKET=None)
    with pytest.raises(RuntimeError, match="LAKE_S3_BUCKET"):
        load_lake_settings()


def test_an_unknown_url_style_is_rejected(environment):
    environment(LAKE_S3_URL_STYLE="sideways")
    with pytest.raises(RuntimeError, match="LAKE_S3_URL_STYLE"):
        load_lake_settings()


def test_the_secret_stays_out_of_the_repr(environment):
    assert "a-very-secret-value" not in repr(load_lake_settings())


def test_a_blank_fred_key_reads_as_missing(environment):
    environment(FRED_API_KEY="   ")
    assert fred_api_key() is None
    environment(FRED_API_KEY="abc123")
    assert fred_api_key() == "abc123"
