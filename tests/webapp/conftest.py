import pytest
import webapp.api as apimod


class _FakeDb:
	def get_all_peeps(self):
		return [
			{"id": 1, "display_name": "Alice", "primary_role": "leader", "active": 1},
			{"id": 2, "display_name": "Bob", "primary_role": "follower", "active": 0},
		]


class _FakeDbManager:
	def __init__(self, *_args, **_kwargs):
		pass
	def __enter__(self):
		return _FakeDb()
	def __exit__(self, exc_type, exc, tb):
		return False


@pytest.fixture(autouse=True)
def fake_db(monkeypatch):
	# Override DbManager used by the API for all tests under tests/webapp/
	monkeypatch.setattr(apimod, "DbManager", _FakeDbManager)
