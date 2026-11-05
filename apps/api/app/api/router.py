from fastapi import APIRouter

from app.api.routes.assistant import router as assistant_router
from app.api.routes.automations import router as automations_router
from app.api.routes.auth import router as auth_router
from app.api.routes.behavior import router as behavior_router
from app.api.routes.dashboard import router as dashboard_router
from app.api.routes.health import router as health_router
from app.api.routes.learning import router as learning_router
from app.api.routes.news import router as news_router
from app.api.routes.onboarding import router as onboarding_router
from app.api.routes.portfolio import router as portfolio_router
from app.api.routes.profile import router as profile_router
from app.api.routes.product import router as product_router
from app.api.routes.simulation import router as simulation_router
from app.api.routes.users import router as users_router

api_router = APIRouter()
api_router.include_router(health_router)
api_router.include_router(product_router)
api_router.include_router(auth_router)
api_router.include_router(assistant_router)
api_router.include_router(automations_router)
api_router.include_router(onboarding_router)
api_router.include_router(users_router)
api_router.include_router(behavior_router)
api_router.include_router(dashboard_router)
api_router.include_router(learning_router)
api_router.include_router(portfolio_router)
api_router.include_router(profile_router)
api_router.include_router(simulation_router)
api_router.include_router(news_router)
