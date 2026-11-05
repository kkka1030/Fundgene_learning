from pydantic import BaseModel


class ProductModule(BaseModel):
    slug: str
    title: str
    summary: str


class ProductResponse(BaseModel):
    name: str
    tagline: str
    phase: str
    modules: list[ProductModule]
