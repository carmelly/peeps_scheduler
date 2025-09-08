from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .api import api
from .views import views


DB_PATH = "db/peeps_scheduler.db"
VERSION = "dev"

app = FastAPI(title="Peeps Scheduler API", version=VERSION)

app.mount("/static", StaticFiles(directory="webapp/static"), name="static")
templates = Jinja2Templates(directory="webapp/templates")

app.include_router(api)
app.include_router(views)
