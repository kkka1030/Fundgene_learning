from collections.abc import Generator
import os

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

os.environ["FUNDGENE_AGENT_MODE"] = "deterministic"
os.environ["FUNDGENE_AUTOMATION_WORKER_ENABLED"] = "false"
os.environ.pop("FUNDGENE_DEEPSEEK_API_KEY", None)
os.environ.pop("DEEPSEEK_API_KEY", None)

from app.core.database import Base, get_db_session
from app.core.config import get_settings
from app.main import app
from app.runtime.deps import get_advisor_runtime


@pytest.fixture(autouse=True)
def clear_cached_settings() -> Generator[None, None, None]:
    get_settings.cache_clear()
    get_advisor_runtime.cache_clear()
    try:
        yield
    finally:
        get_settings.cache_clear()
        get_advisor_runtime.cache_clear()


@pytest.fixture()
def session_factory() -> Generator[sessionmaker[Session], None, None]:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    TestingSessionLocal = sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=engine,
        expire_on_commit=False,
    )
    Base.metadata.create_all(bind=engine)
    try:
        yield TestingSessionLocal
    finally:
        Base.metadata.drop_all(bind=engine)
        engine.dispose()


@pytest.fixture()
def client(session_factory: sessionmaker[Session]) -> Generator[TestClient, None, None]:
    def override_get_db_session() -> Generator[Session, None, None]:
        db = session_factory()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db_session] = override_get_db_session
    try:
        with TestClient(app) as test_client:
            yield test_client
    finally:
        app.dependency_overrides.clear()
