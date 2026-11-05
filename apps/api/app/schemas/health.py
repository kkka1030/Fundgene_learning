from datetime import datetime
from typing import Literal

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: Literal["ok"]
    service: str
    environment: str
    timestamp: datetime


class ReadyResponse(BaseModel):
    status: Literal["ready"]
    service: str
    environment: str
    database: Literal["ok"]
    timestamp: datetime
