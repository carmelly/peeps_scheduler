from fastapi.testclient import TestClient
from webapp.main import app

def test_health():
	client = TestClient(app)
	resp = client.get("/health")
	assert resp.status_code == 200
	assert resp.json() == {"status": "ok"}

def test_version_endpoint():
	client = TestClient(app)
	resp = client.get("/version")
	assert resp.status_code == 200
	body = resp.json()
	assert "version" in body
	assert isinstance(body["version"], str)
	assert body["version"] != ""