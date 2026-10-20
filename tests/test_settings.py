from uk_flood.settings import Settings, get_settings


def test_settings_defaults(monkeypatch):
    monkeypatch.delenv("GCP_PROJECT_ID", raising=False)
    s = Settings()
    assert s.gcp_project_id == "ukfloodpipeline"
    assert s.bq_raw_dataset == "uk_flood_raw"
    assert s.http_max_retries == 5


def test_settings_reads_env(monkeypatch):
    monkeypatch.setenv("GCP_PROJECT_ID", "my-project")
    monkeypatch.setenv("MAX_BYTES_BILLED", "12345")
    s = Settings()
    assert s.gcp_project_id == "my-project"
    assert s.max_bytes_billed == 12345


def test_get_settings_smoke():
    s = get_settings()
    assert s.ea_api_base_url.startswith("https://")
