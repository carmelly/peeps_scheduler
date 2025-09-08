from fastapi.testclient import TestClient
import webapp.main as appmod


def test_health():
	client = TestClient(appmod.app)
	r = client.get("/api/health")
	assert r.status_code == 200
	assert r.json() == {"status": "ok"}


def test_version():
	client = TestClient(appmod.app)
	r = client.get("/api/version")
	assert r.status_code == 200
	assert "version" in r.json()


def test_api_peeps():
	client = TestClient(appmod.app)
	r = client.get("/api/peeps")
	assert r.status_code == 200
	data = r.json()
	assert "peeps" in data
	assert data["peeps"][0]["display_name"] == "Alice"
