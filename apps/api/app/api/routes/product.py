from fastapi import APIRouter

from app.schemas.product import ProductModule, ProductResponse

router = APIRouter(tags=["system"])


@router.get("/product", response_model=ProductResponse)
def get_product() -> ProductResponse:
    return ProductResponse(
        name="FundGene",
        tagline="新手基金投资陪练与决策支持产品",
        phase="phase-4-mvp-spine-c",
        modules=[
            ProductModule(
                slug="onboarding",
                title="Onboarding",
                summary="建立用户基础画像、风险问卷和第一条推荐路径。",
            ),
            ProductModule(
                slug="dashboard",
                title="Dashboard",
                summary="汇总学习、组合和下一步动作的总览工作区。",
            ),
            ProductModule(
                slug="coach",
                title="AI Coach",
                summary="单入口 advisor agent 的结构化问答工作区。",
            ),
            ProductModule(
                slug="learning",
                title="Learning Center",
                summary="基金知识、风险理解与学习路径跟踪。",
            ),
            ProductModule(
                slug="portfolio",
                title="Portfolio Analysis",
                summary="手动录入组合快照并生成可解释分析报告。",
            ),
            ProductModule(
                slug="simulation",
                title="Historical Simulation",
                summary="用固定历史情境训练行为纪律，并把复盘结果回写到产品上下文。",
            ),
        ],
    )
