from fastapi import FastAPI, APIRouter,Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from managers import DbManager

DB_PATH = "db/peeps_scheduler.db"

try:
	from webapp import __version__ as _APP_VERSION
	VERSION = str(_APP_VERSION)
except Exception:
	VERSION = "dev"

app = FastAPI(title="Peeps Scheduler API", version=VERSION)

# serve static files 
app.mount("/static", StaticFiles(directory="webapp/static"), name="static")
templates = Jinja2Templates(directory="webapp/templates")

# -----------------------
# API (JSON) under /api/*
# -----------------------
api = APIRouter(prefix="/api", tags=["api"])

@api.get("/health")
def health():
	return {"status": "ok"}

@api.get("/version")
def api_version():
	return {"version": VERSION}

@api.get("/peeps")
def api_get_peeps():
	with DbManager(DB_PATH) as db:
		peeps = db.get_all_peeps()
	return {"peeps": peeps}

app.include_router(api)

# -----------------------
# Views at root
# -----------------------
@app.get("/peeps", response_class=HTMLResponse)
def peeps_view(request: Request):
	data = api_get_peeps() 
	return templates.TemplateResponse(
		"peeps.html",
		{
			"request": request,
			"title": "Peeps",
			"version": VERSION,
			"peeps": data["peeps"],
		},
	)