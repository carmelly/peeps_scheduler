from fastapi import FastAPI

try:
	from webapp import __version__ as _APP_VERSION
	VERSION = str(_APP_VERSION)
except Exception:
	VERSION = "dev"

app = FastAPI(title="Peeps Scheduler API", version=VERSION)

@app.get("/health")
def health():
	return {"status": "ok"}

@app.get("/version")
def version():
	return {"version": VERSION}