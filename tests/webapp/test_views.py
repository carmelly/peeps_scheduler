from fastapi.testclient import TestClient
import webapp.main as appmod


def test_peeps_view_html_renders():
	client = TestClient(appmod.app)
	r = client.get("/peeps")
	assert r.status_code == 200
	html = r.text
	assert "<table" in html
	assert "Alice" in html