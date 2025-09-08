from fastapi import APIRouter
from managers import DbManager

DB_PATH = "db/peeps_scheduler.db"

api = APIRouter(prefix="/api", tags=["api"])


@api.get("/health")
def health():
	return {"status": "ok"}


@api.get("/version")
def version():
	return {"version": "dev"}


@api.get("/peeps")
def api_get_peeps():
	with DbManager(DB_PATH) as db:
		peeps = db.get_all_peeps()
	return {"peeps": peeps}
