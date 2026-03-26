"""Dashboard FastAPI app — serves API routes and static UI."""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from pipeline.api.routes_status import router as status_router
from pipeline.api.routes_exceptions import router as exceptions_router
from pipeline.api.routes_decisions import router as decisions_router
from pipeline.api.routes_corrections import router as corrections_router
from pipeline.api.routes_retrospective import router as retrospective_router
from pipeline.api.routes_preview import router as preview_router
from pipeline.api.routes_rules import router as rules_router


def create_dashboard_app() -> FastAPI:
    """Create the dashboard FastAPI app."""
    app = FastAPI(title="Pipeline Dashboard")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(status_router, prefix="/api")
    app.include_router(exceptions_router, prefix="/api")
    app.include_router(decisions_router, prefix="/api")
    app.include_router(corrections_router, prefix="/api")
    app.include_router(preview_router, prefix="/api")
    app.include_router(retrospective_router, prefix="/api")
    app.include_router(rules_router, prefix="/api")

    return app


def mount_static(app: FastAPI) -> None:
    """Mount static UI files — call after all routers are added."""
    ui_dist = Path(__file__).resolve().parent.parent.parent / "pipeline-ui" / "dist"
    if ui_dist.exists():
        app.mount("/", StaticFiles(directory=str(ui_dist), html=True), name="ui")
