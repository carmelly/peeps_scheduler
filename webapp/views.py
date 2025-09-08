from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from .api import api_get_peeps

views = APIRouter(tags=["views"])
templates = Jinja2Templates(directory="webapp/templates")


@views.get("/peeps", response_class=HTMLResponse)
def peeps_view(request: Request):
	data = api_get_peeps()
	return templates.TemplateResponse(
		"peeps.html",
		{
			"request": request,
			"title": "Peeps",
			"version": "dev",
			"peeps": data["peeps"],
		},
	)
