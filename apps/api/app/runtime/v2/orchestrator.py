from datetime import datetime, timezone
import re
from time import perf_counter

from sqlalchemy.orm import Session

from app.models.agent_evidence_ref import AgentEvidenceRef
from app.models.agent_run import AgentRun
from app.models.agent_state_update_proposal import AgentStateUpdateProposal
from app.models.agent_step import AgentStep
from app.models.agent_tool_call import AgentToolCall
from app.runtime.action_targets import build_recommended_action_targets
from app.runtime.follow_up_prompts import build_user_follow_up_prompts
from app.runtime.toolchains.base import AdvisorUserContext
from app.runtime.v2.active_runs import AgentRunCancelled
from app.runtime.v2.composer import ResponseComposer
from app.runtime.v2.events import AgentTurnContext
from app.runtime.v2.events import event_phase_for_step
from app.runtime.v2.events import event_title_for_step
from app.runtime.v2.features import RuntimeCapabilityConfig, build_runtime_capabilities
from app.runtime.v2.planner import AgentPlanner
from app.runtime.v2.policy import SafetyPolicy
from app.runtime.v2.schemas import (
    AgentPlan,
    ContextSnapshot,
    EvidenceRef,
    FinalResponseValidationResult,
    OrchestratorRunResult,
    PlanConstraint,
    PlannedToolCall,
    PolicyResult,
    RuntimeTraceEvent,
    SkillSelection,
    ToolResult,
    ToolSelectionSignal,
    WorkerValidationResult,
    WorkerFinding,
    WorkerOutput,
)
from app.runtime.v2.tools.registry import ToolRegistry
from app.runtime.v2.workers import (
    BehaviorWorker,
    LearningWorker,
    NewsWorker,
    PortfolioWorker,
    SimulationWorker,
)
from app.schemas.assistant import AdvisorResponse
from app.services.evidence import EvidenceService


ORCHESTRATOR_VERSION = "agent_runtime_v2"
RISK_NOTICE = "这次回答用于帮你理解风险和整理下一步，不代表收益承诺，也不会替你买卖或下单。"

_INTERNAL_FINDING_PATTERNS = (
    re.compile(r"\s*运行时风险约束：[^。]*(?:。|$)"),
    re.compile(r"\s*影响路径约束：[^。]*(?:。|$)"),
    re.compile(r"\s*检索证据提示：[^。]*(?:。|$)"),
    re.compile(r"\s*如果用户问“[^”]+”，先给出这些具体标题，再提醒它们需要逐条解读。?"),
)


class AdvisorOrchestrator:
    def __init__(
        self,
        *,
        model_name: str,
        agent_mode: str = "hybrid",
        model_timeout_ms: int = 8000,
        runtime_flags: str = "default",
        max_tool_calls: int = 8,
        max_workers: int = 3,
        deepseek_api_key_override: str | None = None,
    ) -> None:
        self.model_name = model_name
        self.agent_mode = agent_mode
        self.capabilities: RuntimeCapabilityConfig = build_runtime_capabilities(
            flags=runtime_flags,
            max_tool_calls=max_tool_calls,
            max_workers=max_workers,
        )
        self.policy = SafetyPolicy()
        self.tools = ToolRegistry()
        self.planner = AgentPlanner(
            tool_registry=self.tools,
            capabilities=self.capabilities,
        )
        self.composer = ResponseComposer(
            model_name=model_name,
            agent_mode=agent_mode,
            timeout_ms=model_timeout_ms,
            risk_notice=RISK_NOTICE,
            deepseek_api_key_override=deepseek_api_key_override,
        )
        self.workers = {
            "learning": LearningWorker(),
            "portfolio": PortfolioWorker(),
            "behavior": BehaviorWorker(),
            "simulation": SimulationWorker(),
            "news": NewsWorker(),
        }
        self._turn_context: AgentTurnContext | None = None

    async def run(
        self,
        *,
        db: Session,
        agent_run: AgentRun,
        session_id: str,
        message: str,
        user_context: AdvisorUserContext,
        page_context: dict[str, object] | None = None,
        turn_context: AgentTurnContext | None = None,
    ) -> OrchestratorRunResult:
        self._turn_context = turn_context
        run_started = perf_counter()
        self._raise_if_cancelled()
        input_policy = self.policy.check_input(message)
        execution_policy = input_policy
        contextual_message = self._contextualize_message(
            message=message,
            page_context=page_context,
        )
        self._record_step(
            db,
            run_id=agent_run.id,
            sequence=1,
            step_name="input_guard",
            input_payload={"message": message, "page_context": page_context},
            output_payload=input_policy.model_dump(mode="json"),
        )
        self._raise_if_cancelled()

        plan = self.planner.build_plan(
            message=contextual_message,
            blocked=input_policy.status == "block_with_guidance",
        )
        intent = plan.primary_intent
        self._record_step(
            db,
            run_id=agent_run.id,
            sequence=2,
            step_name="classify_intent",
            input_payload={"message": message, "page_context": page_context},
            output_payload={
                "intent": intent,
                "detected_intents": plan.detected_intents,
                "method": "keyword_rules_v3_multi_intent",
            },
        )
        self._record_step(
            db,
            run_id=agent_run.id,
            sequence=3,
            step_name="build_agent_plan",
            input_payload={
                "intent": intent,
                "policy_status": input_policy.status,
                "page_context": page_context,
            },
            output_payload=plan.model_dump(mode="json"),
        )
        self._raise_if_cancelled()

        snapshot = self._build_snapshot(user_context)
        evidence_service = EvidenceService(db)
        self._record_step(
            db,
            run_id=agent_run.id,
            sequence=4,
            step_name="load_context_snapshot",
            input_payload={"user_id": user_context.user_id, "page_context": page_context},
            output_payload={
                **snapshot.model_dump(mode="json"),
                "page_context": page_context,
            },
        )
        self._raise_if_cancelled()

        plan_validation = self._validate_plan(
            plan,
            blocked=input_policy.status == "block_with_guidance",
        )
        if any(constraint.status == "fail" for constraint in plan_validation):
            plan = self._safe_plan_after_validation_failure(plan)
            execution_policy = PolicyResult(
                status="runtime_repair",
                reason="agent plan validation failed before tool execution.",
                blocked_terms=["plan_validation_failed"],
            )
            repair_validation = self._validate_plan(plan, blocked=True)
            plan_validation = [
                *plan_validation,
                PlanConstraint(
                    name="plan_repaired_after_validation_failure",
                    status="warn",
                    detail="计划校验失败后已降级到画像和安全边界工具，避免执行未知或越权工具。",
                ),
                *repair_validation,
            ]
            intent = plan.primary_intent
        plan.constraints = [*plan.constraints, *plan_validation]
        self._record_step(
            db,
            run_id=agent_run.id,
            sequence=5,
            step_name="validate_plan",
            input_payload=plan.model_dump(mode="json"),
            output_payload={
                "constraints": [
                    constraint.model_dump(mode="json") for constraint in plan_validation
                ],
                "status": (
                    "fail"
                    if any(item.status == "fail" for item in plan_validation)
                    else "pass"
                ),
            },
        )
        self._raise_if_cancelled()

        selected_tools = plan.tool_names
        self._record_step(
            db,
            run_id=agent_run.id,
            sequence=6,
            step_name="select_tools",
            input_payload={"intent": intent, "policy_status": execution_policy.status},
            output_payload={
                "tools": selected_tools,
                "worker_names": plan.worker_names,
                "selected_skills": [
                    skill.model_dump(mode="json") for skill in plan.selected_skills
                ],
                "tool_budget": {
                    "max_tool_calls": plan.max_tool_calls,
                    "planned_tool_calls": len(selected_tools),
                },
            },
        )
        self._raise_if_cancelled()

        tool_step = self._record_step(
            db,
            run_id=agent_run.id,
            sequence=7,
            step_name="execute_tools",
            input_payload={"intent": intent, "tools": selected_tools},
            output_payload={"status": "running"},
            status="running",
            completed=False,
        )
        tool_results = []
        for tool_name in selected_tools:
            self._raise_if_cancelled()
            tool_results.append(
                self._execute_tool(
                    db,
                    run_id=agent_run.id,
                    step_id=tool_step.id,
                    tool_name=tool_name,
                    snapshot=snapshot,
                    query=contextual_message,
                    evidence_service=evidence_service,
                )
            )
            self._raise_if_cancelled()
        self._complete_step(
            tool_step,
            output_payload={
                "tools": selected_tools,
                "status": "completed",
                "result_count": len(tool_results),
                "evidence_ref_count": sum(
                    len(result.evidence_refs) for result in tool_results
                ),
            },
        )
        self._raise_if_cancelled()

        worker_step = self._record_step(
            db,
            run_id=agent_run.id,
            sequence=8,
            step_name="execute_workers",
            input_payload={
                "intent": intent,
                "worker_names": plan.worker_names,
                "tools": selected_tools,
                "selected_skills": [
                    skill.model_dump(mode="json") for skill in plan.selected_skills
                ],
            },
            output_payload={"status": "running"},
            status="running",
            completed=False,
        )
        worker_outputs = self._run_workers(
            plan=plan,
            message=contextual_message,
            tool_results=tool_results,
            input_policy=execution_policy,
        )
        self._raise_if_cancelled()
        worker_output = self._aggregate_worker_outputs(
            plan=plan,
            worker_outputs=worker_outputs,
            input_policy=execution_policy,
        )
        self._raise_if_cancelled()
        self._persist_evidence_refs(
            db,
            run_id=agent_run.id,
            step_id=worker_step.id,
            worker_output=worker_output,
        )
        self._persist_state_update_proposals(
            db,
            run_id=agent_run.id,
            worker_output=worker_output,
        )
        self._raise_if_cancelled()
        self._complete_step(
            worker_step,
            output_payload={
                **worker_output.model_dump(mode="json"),
                "worker_outputs": [
                    output.model_dump(mode="json") for output in worker_outputs
                ],
            },
        )

        worker_validation = self._validate_worker_output(
            worker_output=worker_output,
            input_policy=execution_policy,
        )
        self._record_step(
            db,
            run_id=agent_run.id,
            sequence=9,
            step_name="validate_worker_output",
            input_payload=worker_output.model_dump(mode="json"),
            output_payload=worker_validation.model_dump(mode="json"),
        )
        self._raise_if_cancelled()

        deterministic_response = self._compose_deterministic_response(
            intent=intent,
            worker_output=worker_output,
            input_policy=execution_policy,
        )
        self._raise_if_cancelled()
        compose_result = await self.composer.compose(
            intent=intent,
            worker_output=worker_output,
            input_policy=execution_policy,
            deterministic_response=deterministic_response,
        )
        self._raise_if_cancelled()
        draft_response = compose_result.response
        output_policy = (
            execution_policy
            if execution_policy.status in {"block_with_guidance", "runtime_repair"}
            else self.policy.check_output(draft_response.answer)
        )
        self._record_step(
            db,
            run_id=agent_run.id,
            sequence=10,
            step_name="policy_guard",
            input_payload={"intent": intent},
            output_payload=output_policy.model_dump(mode="json"),
        )
        self._raise_if_cancelled()
        if output_policy.status == "revise" and output_policy.revised_answer:
            draft_response.answer = output_policy.revised_answer

        if self.capabilities.enabled("strict_final_validation"):
            draft_response, final_validation = (
                self._normalize_and_validate_final_response(
                    response=draft_response,
                    worker_output=worker_output,
                    policy_status=output_policy.status,
                )
            )
        else:
            final_validation = FinalResponseValidationResult(
                status="pass",
                citation_count=len(draft_response.citations),
            )

        self._record_step(
            db,
            run_id=agent_run.id,
            sequence=11,
            step_name="compose_response",
            input_payload={
                "worker_output": worker_output.model_dump(mode="json"),
                "composer": compose_result.metadata,
            },
            output_payload=draft_response.model_dump(mode="json"),
        )
        self._record_step(
            db,
            run_id=agent_run.id,
            sequence=12,
            step_name="validate_final_response",
            input_payload=draft_response.model_dump(mode="json"),
            output_payload=final_validation.model_dump(mode="json"),
        )
        self._raise_if_cancelled()

        latency_ms = self._elapsed_ms(run_started)
        return OrchestratorRunResult(
            response=draft_response,
            intent=intent,
            run_status="completed",
            policy_status=output_policy.status,
            context_snapshot=snapshot,
            latency_ms=latency_ms,
            tool_trace={
                "orchestrator_version": ORCHESTRATOR_VERSION,
                "intent": intent,
                "policy_status": output_policy.status,
                "page_context": page_context,
                "tools": selected_tools,
                "skills": [
                    skill.model_dump(mode="json") for skill in plan.selected_skills
                ],
                "selected_skill_versions": [
                    f"{skill.skill_name}@{skill.skill_version}"
                    for skill in plan.selected_skills
                ],
                "runtime_capabilities": self.capabilities.model_dump(mode="json"),
                "agent_plan": plan.model_dump(mode="json"),
                "worker": worker_output.worker_name,
                "workers": plan.worker_names,
                "worker_validation": worker_validation.model_dump(mode="json"),
                "final_response_validation": final_validation.model_dump(mode="json"),
                "trace_events": self._build_trace_events(
                    plan=plan,
                    tool_results=tool_results,
                    worker_outputs=worker_outputs,
                    worker_validation=worker_validation,
                    final_validation=final_validation,
                    output_policy=output_policy,
                    composer_metadata=compose_result.metadata,
                ),
                "session_id": session_id,
                "composer": compose_result.metadata,
            },
            fallback_reason=compose_result.fallback_reason,
        )

    def _contextualize_message(
        self,
        *,
        message: str,
        page_context: dict[str, object] | None,
    ) -> str:
        if not page_context:
            return message

        fragments: list[str] = []
        from_route = page_context.get("from_route")
        focus = page_context.get("focus")
        daily_brief_id = page_context.get("daily_brief_id")
        source_ids = page_context.get("source_ids")
        if isinstance(from_route, str) and from_route:
            fragments.append(f"来源页面={from_route}")
        if isinstance(focus, str) and focus:
            fragments.append(f"当前焦点={focus}")
        if isinstance(daily_brief_id, str) and daily_brief_id:
            fragments.append(f"Daily Brief={daily_brief_id}")
        if isinstance(source_ids, dict) and source_ids:
            clean_source_ids = {
                str(key): str(value)
                for key, value in source_ids.items()
                if value is not None
            }
            if clean_source_ids:
                fragments.append(f"关联来源={clean_source_ids}")

        if not fragments:
            return message
        return f"{message}\n\n[页面上下文：{'; '.join(fragments)}]"

    def _run_worker(
        self,
        *,
        intent: str,
        message: str,
        tool_results: list[ToolResult],
        input_policy: PolicyResult,
        skills: list[SkillSelection] | None = None,
    ) -> WorkerOutput:
        if input_policy.status in {"block_with_guidance", "runtime_repair"}:
            evidence_refs = [
                evidence
                for tool_result in tool_results
                for evidence in tool_result.evidence_refs
            ]
            runtime_repair = input_policy.status == "runtime_repair"
            finding = (
                "内部计划校验没有通过，已退回到只读安全上下文；本次不会执行未注册、"
                "越权或超预算工具。"
                if runtime_repair
                else "用户请求触及交易执行、收益保证或账户连接边界，需要转为学习与风险理解。"
            )
            safety_boundary = (
                "Runtime guardrail blocks invalid plans before tool execution."
                if runtime_repair
                else "FundGene 不提供买卖、清仓、满仓、保本收益或自动交易指令。"
            )
            return WorkerOutput(
                worker_name="SafetyPolicyWorker",
                intent="learning",
                skill_names=[],
                findings=[finding],
                structured_findings=[
                    WorkerFinding(
                        claim=finding,
                        explanation="输入 guard 命中产品边界后，只允许生成学习型引导。",
                        evidence_refs=self._evidence_keys(evidence_refs),
                        support_level="contextual",
                        safety_boundary=safety_boundary,
                    )
                ],
                evidence_refs=evidence_refs,
                risk_flags=[input_policy.reason],
                recommended_actions=[
                    (
                        "稍后重试该问题；当前回答已限制在安全只读上下文。"
                        if runtime_repair
                        else "把问题改写成学习目标，例如“我该如何理解这个基金的风险”。"
                    ),
                    (
                        "如果反复出现，请检查 Skill Registry 和 ToolRegistry 的工具预算。"
                        if runtime_repair
                        else "回到 Portfolio 或 Learning，先检查风险暴露和基础概念。"
                    ),
                ],
                confidence=0.95,
                limitations=[
                    (
                        "这是内部 runtime guardrail 触发，不代表用户请求本身越界。"
                        if runtime_repair
                        else "安全边界判断由规则 guard 完成。"
                    )
                ],
            )
        worker = self.workers.get(intent, self.workers["learning"])
        return worker.run(message=message, tool_results=tool_results, skills=skills)

    def _run_workers(
        self,
        *,
        plan: AgentPlan,
        message: str,
        tool_results: list[ToolResult],
        input_policy: PolicyResult,
    ) -> list[WorkerOutput]:
        if input_policy.status in {"block_with_guidance", "runtime_repair"}:
            return [
                self._run_worker(
                    intent="learning",
                    message=message,
                    tool_results=tool_results,
                    input_policy=input_policy,
                    skills=[],
                )
            ]

        outputs: list[WorkerOutput] = []
        worker_intents = {
            "LearningWorker": "learning",
            "PortfolioWorker": "portfolio",
            "BehaviorWorker": "behavior",
            "SimulationWorker": "simulation",
            "NewsWorker": "news",
        }
        for worker_name in plan.worker_names:
            intent = worker_intents.get(worker_name)
            if intent is None:
                continue
            outputs.append(
                self._run_worker(
                    intent=intent,
                    message=message,
                    tool_results=tool_results,
                    input_policy=input_policy,
                    skills=self._skills_for_intent(plan.selected_skills, intent),
                )
            )
        if not outputs:
            outputs.append(
                self._run_worker(
                    intent=plan.primary_intent,
                    message=message,
                    tool_results=tool_results,
                    input_policy=input_policy,
                    skills=self._skills_for_intent(
                        plan.selected_skills,
                        plan.primary_intent,
                    ),
                )
            )
        return outputs

    def _aggregate_worker_outputs(
        self,
        *,
        plan: AgentPlan,
        worker_outputs: list[WorkerOutput],
        input_policy: PolicyResult,
    ) -> WorkerOutput:
        if len(worker_outputs) == 1:
            output = worker_outputs[0]
            output.intent = plan.primary_intent
            return output

        findings = [finding for output in worker_outputs for finding in output.findings]
        structured_findings = [
            finding
            for output in worker_outputs
            for finding in output.structured_findings
        ]
        evidence_refs = [
            evidence for output in worker_outputs for evidence in output.evidence_refs
        ]
        risk_flags = list(
            dict.fromkeys(
                flag for output in worker_outputs for flag in output.risk_flags
            )
        )
        skill_names = list(
            dict.fromkeys(
                skill_name
                for output in worker_outputs
                for skill_name in output.skill_names
            )
        )
        learning_outcomes = [
            output.learning_outcome
            for output in worker_outputs
            if output.learning_outcome
        ]
        skill_output_guidance = list(
            dict.fromkeys(
                guidance
                for output in worker_outputs
                for guidance in output.skill_output_guidance
            )
        )
        skill_forbidden_language = list(
            dict.fromkeys(
                term
                for output in worker_outputs
                for term in output.skill_forbidden_language
            )
        )
        recommended_actions = list(
            dict.fromkeys(
                action
                for output in worker_outputs
                for action in output.recommended_actions
            )
        )[:4]
        state_update_proposals = [
            proposal
            for output in worker_outputs
            for proposal in output.state_update_proposals
        ]
        limitations = list(
            dict.fromkeys(
                limitation
                for output in worker_outputs
                for limitation in output.limitations
            )
        )
        limitations.append("本次回答由多个内部 worker 聚合，仍只输出一个用户可读答案。")
        confidence = min(output.confidence for output in worker_outputs)
        if input_policy.status != "allow":
            confidence = min(confidence, 0.7)

        return WorkerOutput(
            worker_name="AdvisorSynthesisWorker",
            intent=plan.primary_intent,
            skill_names=skill_names,
            learning_outcome="；".join(dict.fromkeys(learning_outcomes)) or None,
            skill_output_guidance=skill_output_guidance,
            skill_forbidden_language=skill_forbidden_language,
            findings=findings,
            structured_findings=structured_findings,
            evidence_refs=evidence_refs,
            risk_flags=risk_flags,
            recommended_actions=recommended_actions,
            state_update_proposals=state_update_proposals,
            confidence=confidence,
            limitations=limitations,
        )

    def _safe_plan_after_validation_failure(self, plan: AgentPlan) -> AgentPlan:
        return AgentPlan(
            primary_intent="learning",
            detected_intents=["learning"],
            worker_names=["SafetyPolicyWorker"],
            selected_skills=[],
            planned_tools=[
                PlannedToolCall(
                    tool_name="profile.current",
                    purpose="计划校验失败后读取画像以保持安全上下文。",
                ),
                PlannedToolCall(
                    tool_name="safety.boundary_rules",
                    purpose="计划校验失败后读取产品安全边界。",
                ),
            ],
            tool_selection_signals=[
                ToolSelectionSignal(
                    tool_name="profile.current",
                    score=100,
                    source="required",
                    reason="计划校验失败后的安全降级工具。",
                ),
                ToolSelectionSignal(
                    tool_name="safety.boundary_rules",
                    score=100,
                    source="required",
                    reason="计划校验失败后的产品边界工具。",
                ),
            ],
            constraints=plan.constraints,
            max_tool_calls=2,
            requires_human_review=True,
            rationale="原计划校验失败，已降级为安全边界回复。",
        )

    def _skills_for_intent(
        self,
        skills: list[SkillSelection],
        intent: str,
    ) -> list[SkillSelection]:
        hints_by_intent = {
            "learning": ("fund_basics",),
            "portfolio": ("portfolio",),
            "behavior": ("behavior",),
            "simulation": ("simulation",),
            "news": ("news",),
        }
        hints = hints_by_intent.get(intent, ())
        scoped = [
            skill
            for skill in skills
            if skill.skill_name == "cross_domain_synthesis_v1"
            or any(hint in skill.skill_name for hint in hints)
        ]
        return scoped or skills

    def _validate_plan(
        self,
        plan: AgentPlan,
        *,
        blocked: bool,
    ) -> list[PlanConstraint]:
        constraints: list[PlanConstraint] = []
        known_tools = set(self.tools.definitions)
        unknown_tools = [tool for tool in plan.tool_names if tool not in known_tools]
        constraints.append(
            PlanConstraint(
                name="known_tools",
                status="fail" if unknown_tools else "pass",
                detail=(
                    f"未知工具：{', '.join(unknown_tools)}"
                    if unknown_tools
                    else "所有 planned tools 都在 ToolRegistry 中注册。"
                ),
            )
        )

        total_budget = sum(
            self.tools.definitions[tool].budget_cost
            for tool in plan.tool_names
            if tool in self.tools.definitions
        )
        constraints.append(
            PlanConstraint(
                name="tool_budget",
                status="pass" if total_budget <= plan.max_tool_calls else "fail",
                detail=f"计划工具预算 {total_budget}/{plan.max_tool_calls}。",
            )
        )

        non_read_tools = [
            tool
            for tool in plan.tool_names
            if tool in self.tools.definitions
            and self.tools.definitions[tool].permission_level != "read"
        ]
        constraints.append(
            PlanConstraint(
                name="read_only_permission",
                status="fail" if non_read_tools else "pass",
                detail=(
                    f"发现非只读工具：{', '.join(non_read_tools)}"
                    if non_read_tools
                    else "所有工具都是 read 权限。"
                ),
            )
        )

        intent_mismatches = []
        for tool in plan.tool_names:
            definition = self.tools.definitions.get(tool)
            if definition is None or not definition.allowed_intents:
                continue
            if not any(
                intent in definition.allowed_intents for intent in plan.detected_intents
            ):
                intent_mismatches.append(tool)
        constraints.append(
            PlanConstraint(
                name="allowed_intents",
                status="fail" if intent_mismatches else "pass",
                detail=(
                    f"工具不匹配当前意图：{', '.join(intent_mismatches)}"
                    if intent_mismatches
                    else "所有工具均匹配至少一个 detected intent。"
                ),
            )
        )

        missing_skill_tools = sorted(
            {
                tool
                for skill in plan.selected_skills
                for tool in skill.required_tools
                if tool not in plan.tool_names
            }
        )
        constraints.append(
            PlanConstraint(
                name="skill_required_tools",
                status="fail" if missing_skill_tools else "pass",
                detail=(
                    f"已选 skill 缺少 required tools：{', '.join(missing_skill_tools)}"
                    if missing_skill_tools
                    else "所有已选 skill 的 required tools 均包含在 planned tools 中。"
                ),
            )
        )

        if blocked:
            unsafe_tools = [
                tool
                for tool in plan.tool_names
                if tool not in {"profile.current", "safety.boundary_rules"}
            ]
            constraints.append(
                PlanConstraint(
                    name="blocked_request_tool_scope",
                    status="fail" if unsafe_tools else "pass",
                    detail=(
                        f"安全拦截请求不应执行：{', '.join(unsafe_tools)}"
                        if unsafe_tools
                        else "安全拦截请求只执行画像和边界规则工具。"
                    ),
                )
            )
        return constraints

    def _validate_worker_output(
        self,
        *,
        worker_output: WorkerOutput,
        input_policy: PolicyResult,
    ) -> WorkerValidationResult:
        issues: list[str] = []
        backed = 0
        unsupported = 0
        for finding in worker_output.structured_findings:
            if finding.support_level == "unsupported" or not finding.evidence_refs:
                unsupported += 1
                issues.append(f"unsupported_claim:{finding.claim[:80]}")
            else:
                backed += 1

        status = "pass"
        if unsupported:
            status = "warn"
        if (
            input_policy.status in {"block_with_guidance", "runtime_repair"}
            and worker_output.worker_name != "SafetyPolicyWorker"
        ):
            status = "fail"
            issues.append("guardrail_input_not_handled_by_safety_worker")

        return WorkerValidationResult(
            status=status,
            findings_count=len(worker_output.structured_findings),
            evidence_backed_findings=backed,
            unsupported_findings=unsupported,
            worker_names=[worker_output.worker_name],
            issues=issues,
        )

    def _normalize_and_validate_final_response(
        self,
        *,
        response: AdvisorResponse,
        worker_output: WorkerOutput,
        policy_status: str,
    ) -> tuple[AdvisorResponse, FinalResponseValidationResult]:
        supported_citations = set(self._evidence_keys(worker_output.evidence_refs))
        supported_citations.add("agent_runtime_v2")
        citations = list(dict.fromkeys(response.citations or []))
        unsupported_citations = [
            citation for citation in citations if citation not in supported_citations
        ]
        normalized_citations = [
            citation for citation in citations if citation in supported_citations
        ] or ["agent_runtime_v2"]

        safe_actions: list[str] = []
        removed_unsafe_actions: list[str] = []
        for action in response.recommended_actions:
            if self.policy.check_output(action).status == "allow":
                safe_actions.append(action)
            else:
                removed_unsafe_actions.append(action)
        if not safe_actions and response.recommended_actions:
            safe_actions = ["回到 FundGene 学习或组合页，以学习和风险检查方式继续。"]

        normalized = response.model_copy(
            update={
                "answer": self._sanitize_user_visible_text(response.answer),
                "risk_notice": self._sanitize_user_visible_text(response.risk_notice),
                "citations": normalized_citations,
                "recommended_actions": safe_actions,
                "recommended_action_targets": build_recommended_action_targets(
                    intent=response.intent,
                    actions=safe_actions,
                ),
            }
        )
        issues: list[str] = []
        if unsupported_citations:
            issues.append("unsupported_citations_removed")
        if removed_unsafe_actions:
            issues.append("unsafe_actions_removed")
        final_text = " ".join(
            [
                normalized.answer,
                *normalized.recommended_actions,
                *normalized.follow_up_questions,
            ]
        )
        skill_forbidden_terms = [
            term
            for term in worker_output.skill_forbidden_language
            if term and term.lower() in final_text.lower()
        ]
        if skill_forbidden_terms:
            issues.append(
                "skill_forbidden_language:"
                + ",".join(dict.fromkeys(skill_forbidden_terms))
            )
        final_policy = (
            PolicyResult(
                status="allow",
                reason="guardrail 回复允许引用边界术语来说明拒答或降级原因。",
            )
            if policy_status in {"block_with_guidance", "runtime_repair"}
            else self.policy.check_output(final_text)
        )
        if final_policy.status != "allow":
            issues.append("unsafe_final_text")

        status = "pass"
        if issues:
            status = "warn"
        if final_policy.status != "allow":
            status = "fail"
        return normalized, FinalResponseValidationResult(
            status=status,
            citation_count=len(normalized.citations),
            unsupported_citations=unsupported_citations,
            removed_unsafe_actions=removed_unsafe_actions,
            issues=issues,
        )

    def _build_trace_events(
        self,
        *,
        plan: AgentPlan,
        tool_results: list[ToolResult],
        worker_outputs: list[WorkerOutput],
        worker_validation: WorkerValidationResult,
        final_validation: FinalResponseValidationResult,
        output_policy: PolicyResult,
        composer_metadata: dict,
    ) -> list[dict]:
        if not self.capabilities.enabled("trace_observability"):
            return []
        events = [
            RuntimeTraceEvent(
                name="agent.plan",
                kind="plan",
                status="completed",
                attributes={
                    "primary_intent": plan.primary_intent,
                    "detected_intents": plan.detected_intents,
                    "worker_count": len(plan.worker_names),
                    "planned_tool_count": len(plan.planned_tools),
                    "tool_selection_mode": (
                        "scored"
                        if self.capabilities.enabled("scored_tool_discovery")
                        else "intent_fallback"
                    ),
                },
            ),
            RuntimeTraceEvent(
                name="agent.skills",
                kind="plan",
                status="completed",
                attributes={
                    "skill_count": len(plan.selected_skills),
                    "selected_skill_versions": [
                        f"{skill.skill_name}@{skill.skill_version}"
                        for skill in plan.selected_skills
                    ],
                    "required_tools": list(
                        dict.fromkeys(
                            tool
                            for skill in plan.selected_skills
                            for tool in skill.required_tools
                        )
                    ),
                },
            ),
            RuntimeTraceEvent(
                name="agent.tools",
                kind="tool",
                status="completed",
                attributes={
                    "tool_count": len(tool_results),
                    "evidence_ref_count": sum(
                        len(result.evidence_refs) for result in tool_results
                    ),
                    "tool_names": [result.tool_name for result in tool_results],
                },
            ),
            RuntimeTraceEvent(
                name="agent.workers",
                kind="worker",
                status=(
                    "warn"
                    if worker_validation.status == "warn"
                    else "failed"
                    if worker_validation.status == "fail"
                    else "completed"
                ),
                attributes={
                    "worker_names": [output.worker_name for output in worker_outputs],
                    "validation_status": worker_validation.status,
                },
            ),
            RuntimeTraceEvent(
                name="agent.guardrails",
                kind="guardrail",
                status=(
                    "failed"
                    if final_validation.status == "fail"
                    else "warn"
                    if output_policy.status != "allow"
                    or final_validation.status == "warn"
                    else "completed"
                ),
                attributes={
                    "policy_status": output_policy.status,
                    "final_validation_status": final_validation.status,
                    "issues": final_validation.issues,
                },
            ),
            RuntimeTraceEvent(
                name="agent.composer",
                kind="composer",
                status="completed",
                attributes=composer_metadata,
            ),
        ]
        return [event.model_dump(mode="json") for event in events]

    def _compose_deterministic_response(
        self,
        *,
        intent: str,
        worker_output: WorkerOutput,
        input_policy: PolicyResult,
    ) -> AdvisorResponse:
        evidence_for_citations = worker_output.evidence_refs
        if intent == "news":
            news_evidence = [
                evidence
                for evidence in worker_output.evidence_refs
                if evidence.source_type == "news_analysis" and evidence.source_id
            ]
            if news_evidence:
                evidence_for_citations = news_evidence

        raw_citations = [
            f"{evidence.source_type}:{evidence.source_id}"
            if evidence.source_id
            else evidence.source_type
            for evidence in evidence_for_citations[:4]
        ]
        citations = list(dict.fromkeys(raw_citations))
        if input_policy.status in {"block_with_guidance", "runtime_repair"}:
            answer = (
                "内部计划校验没有通过，所以这次先退回到安全的只读上下文；"
                "我不会执行未注册、越权或超预算工具。请稍后重试，或先把问题聚焦到基金概念、"
                "风险承受、组合结构或行为纪律。"
                if input_policy.status == "runtime_repair"
                else (
                    "这个问题已经越过 FundGene 的产品边界：我不能给出买卖、清仓、满仓、"
                    "保本收益或自动交易相关指令。可以继续帮你把它改成学习问题，先看风险、期限、"
                    "组合结构和行为冲动。"
                )
            )
            intent = "learning"
        else:
            answer = self._public_answer_from_findings(worker_output.findings)

        return AdvisorResponse(
            answer=answer,
            intent=intent,
            citations=citations or ["agent_runtime_v2"],
            risk_notice=RISK_NOTICE,
            recommended_actions=worker_output.recommended_actions,
            follow_up_questions=self._follow_up_questions(
                intent,
                " ".join(worker_output.findings),
            ),
        )

    def _public_answer_from_findings(self, findings: list[str]) -> str:
        answer = " ".join(finding.strip() for finding in findings if finding.strip())
        answer = self._sanitize_user_visible_text(answer)
        return answer or "我会先基于你的画像、组合和最近资料解释风险来源，再给出一个安全的下一步。"

    def _sanitize_user_visible_text(self, text: str) -> str:
        answer = text.replace("直接回答：", "")
        for pattern in _INTERNAL_FINDING_PATTERNS:
            answer = pattern.sub(" ", answer)
        answer = re.sub(r"\s+", " ", answer).strip()
        return answer

    def _execute_tool(
        self,
        db: Session,
        *,
        run_id: str,
        step_id: str,
        tool_name: str,
        snapshot: ContextSnapshot,
        query: str,
        evidence_service: EvidenceService,
    ) -> ToolResult:
        started = perf_counter()
        started_at = datetime.now(timezone.utc)
        status = "completed"
        error = None
        output_payload = None
        self._emit_event(
            run_id=run_id,
            event_type="tool_call_started",
            phase="tool",
            title=f"读取工具：{tool_name}",
            status="running",
            at=started_at,
            payload={
                "tool_name": tool_name,
                "permission_level": "read",
                "step_id": step_id,
            },
        )
        try:
            result = self.tools.execute(
                tool_name=tool_name,
                snapshot=snapshot,
                query=query,
                evidence_service=evidence_service,
            )
            output_payload = result.output_payload
            return result
        except Exception as exc:  # pragma: no cover - defensive trace guard
            status = "failed"
            error = str(exc)
            raise
        finally:
            completed_at = datetime.now(timezone.utc)
            latency_ms = self._elapsed_ms(started)
            tool_call = AgentToolCall(
                run_id=run_id,
                step_id=step_id,
                tool_name=tool_name,
                permission_level="read",
                input_payload={"snapshot_version": snapshot.schema_version},
                output_payload=output_payload,
                status=status,
                error=error,
                latency_ms=latency_ms,
                started_at=started_at,
                completed_at=completed_at,
                created_at=started_at,
            )
            db.add(tool_call)
            db.flush()
            self._emit_event(
                run_id=run_id,
                event_type="tool_call_completed",
                phase="tool",
                title=f"读取工具：{tool_name}",
                status=status,
                at=completed_at,
                duration_ms=latency_ms,
                payload={
                    "tool_call_id": tool_call.id,
                    "tool_name": tool_name,
                    "permission_level": "read",
                    "step_id": step_id,
                    "input": tool_call.input_payload,
                    "output": output_payload,
                    "error": error,
                },
            )

    def _record_step(
        self,
        db: Session,
        *,
        run_id: str,
        sequence: int,
        step_name: str,
        input_payload: dict | None = None,
        output_payload: dict | None = None,
        status: str = "completed",
        completed: bool = True,
    ) -> AgentStep:
        now = datetime.now(timezone.utc)
        step = AgentStep(
            run_id=run_id,
            step_name=step_name,
            sequence=sequence,
            status=status,
            input_payload=input_payload,
            output_payload=output_payload,
            started_at=now,
            completed_at=now if completed else None,
            latency_ms=0 if completed else None,
            created_at=now,
        )
        db.add(step)
        db.flush()
        self._emit_step_event(
            event_type="step_started",
            step=step,
            at=now,
            payload={
                "step_id": step.id,
                "step_name": step_name,
                "input": input_payload,
            },
            status="running",
        )
        if completed:
            self._emit_step_event(
                event_type="step_completed",
                step=step,
                at=now,
                duration_ms=step.latency_ms,
                payload={
                    "step_id": step.id,
                    "step_name": step_name,
                    "input": input_payload,
                    "output": output_payload,
                    "error": step.error,
                },
                status=status,
            )
        return step

    def _complete_step(self, step: AgentStep, *, output_payload: dict) -> None:
        now = datetime.now(timezone.utc)
        step.status = "completed"
        step.output_payload = output_payload
        step.completed_at = now
        if step.started_at:
            step.latency_ms = max(
                0,
                round((now - step.started_at).total_seconds() * 1000),
            )
        self._emit_step_event(
            event_type="step_completed",
            step=step,
            at=now,
            duration_ms=step.latency_ms,
            payload={
                "step_id": step.id,
                "step_name": step.step_name,
                "input": step.input_payload,
                "output": output_payload,
                "error": step.error,
            },
            status=step.status,
        )

    def _emit_step_event(
        self,
        *,
        event_type: str,
        step: AgentStep,
        at: datetime,
        payload: dict,
        status: str,
        duration_ms: int | None = None,
    ) -> None:
        self._emit_event(
            run_id=step.run_id,
            event_type=event_type,
            phase=event_phase_for_step(step.step_name),
            title=event_title_for_step(step.step_name),
            status=status,
            at=at,
            duration_ms=duration_ms,
            payload=payload,
        )

    def _emit_event(
        self,
        *,
        run_id: str,
        event_type: str,
        phase: str,
        title: str,
        status: str,
        at: datetime | None = None,
        duration_ms: int | None = None,
        payload: dict | None = None,
    ) -> None:
        if self._turn_context is None:
            return
        self._turn_context.event_sink.emit(
            run_id=run_id,
            event_type=event_type,
            phase=phase,
            title=title,
            status=status,
            at=at,
            duration_ms=duration_ms,
            payload=payload or {},
        )

    def _raise_if_cancelled(self) -> None:
        token = (
            self._turn_context.cancellation_token
            if self._turn_context is not None
            else None
        )
        if token is not None and token.cancel_requested:
            if token.mark_cancel_requested_event_emitted():
                self._emit_event(
                    run_id=self._turn_context.run_id,
                    event_type="turn_cancel_requested",
                    phase="turn",
                    title="正在停止本次整理",
                    status="cancelling",
                    payload={"reason": "user_requested_cancel"},
                )
            self._emit_event(
                run_id=self._turn_context.run_id,
                event_type="turn_aborted",
                phase="turn",
                title="已停止本次整理",
                status="cancelled",
                payload={
                    "reason": "user_requested_cancel",
                    "message": "用户停止了本次整理。",
                },
            )
            raise AgentRunCancelled("user_requested_cancel")

    def _persist_evidence_refs(
        self,
        db: Session,
        *,
        run_id: str,
        step_id: str,
        worker_output: WorkerOutput,
    ) -> None:
        for evidence in worker_output.evidence_refs:
            db.add(
                AgentEvidenceRef(
                    run_id=run_id,
                    step_id=step_id,
                    worker_name=worker_output.worker_name,
                    source_type=evidence.source_type,
                    source_id=evidence.source_id,
                    source_version=evidence.source_version,
                    quote_or_summary=evidence.quote_or_summary,
                    claim=evidence.claim,
                    support_summary=evidence.support_summary,
                )
            )
        db.flush()

    def _persist_state_update_proposals(
        self,
        db: Session,
        *,
        run_id: str,
        worker_output: WorkerOutput,
    ) -> None:
        for proposal in worker_output.state_update_proposals:
            db.add(
                AgentStateUpdateProposal(
                    run_id=run_id,
                    target_type=proposal.target_type,
                    target_id=proposal.target_id,
                    patch_payload=proposal.patch_payload,
                    reason=proposal.reason,
                    validator_status=proposal.validator_status,
                    validator_message=proposal.validator_message,
                )
            )
        db.flush()

    def _build_snapshot(self, user_context: AdvisorUserContext) -> ContextSnapshot:
        return ContextSnapshot(
            user_id=user_context.user_id,
            display_name=user_context.display_name,
            investing_experience=user_context.investing_experience,
            primary_goal=user_context.primary_goal,
            risk_level=user_context.risk_level,
            bias_tags=user_context.bias_tags,
            learning_progress_percentage=user_context.learning_progress_percentage,
            learning_completed_courses_count=user_context.learning_completed_courses_count,
            learning_total_courses=user_context.learning_total_courses,
            learning_recommended_course_slug=user_context.learning_recommended_course_slug,
            learning_recommended_course_title=user_context.learning_recommended_course_title,
            portfolio_has_report=user_context.portfolio_has_report,
            portfolio_latest_summary=user_context.portfolio_latest_summary,
            portfolio_latest_snapshot_date=user_context.portfolio_latest_snapshot_date,
            behavior_training_focus=user_context.behavior_training_focus,
            behavior_training_guidance=user_context.behavior_training_guidance,
            simulation_recommended_scenario_slug=user_context.simulation_recommended_scenario_slug,
            simulation_recommended_scenario_title=user_context.simulation_recommended_scenario_title,
            simulation_completed_sessions_count=user_context.simulation_completed_sessions_count,
            simulation_latest_review_summary=user_context.simulation_latest_review_summary,
            news_has_analysis=user_context.news_has_analysis,
            news_latest_analysis_id=user_context.news_latest_analysis_id,
            news_latest_title=user_context.news_latest_title,
            news_latest_source_name=user_context.news_latest_source_name,
            news_latest_beginner_translation=user_context.news_latest_beginner_translation,
            news_latest_recommended_action=user_context.news_latest_recommended_action,
        )

    def _detect_intent(self, message: str) -> str:
        normalized = message.lower()
        if any(
            keyword in normalized
            for keyword in (
                "portfolio",
                "allocation",
                "holding",
                "holdings",
                "仓位",
                "持仓",
                "配置",
                "集中",
            )
        ):
            return "portfolio"
        if any(
            keyword in normalized
            for keyword in (
                "behavior",
                "bias",
                "emotion",
                "panic",
                "画像",
                "标签",
                "追涨",
                "偏差",
                "情绪",
                "恐慌",
                "冲动",
            )
        ):
            return "behavior"
        if any(
            keyword in normalized
            for keyword in ("news", "policy", "macro", "新闻", "政策", "宏观", "加息")
        ):
            return "news"
        if any(
            keyword in normalized
            for keyword in ("simulation", "scenario", "情景", "情境", "演练", "复盘")
        ):
            return "simulation"
        return "learning"

    def _follow_up_questions(self, intent: str, message: str = "") -> list[str]:
        return build_user_follow_up_prompts(intent, message)

    def _elapsed_ms(self, started: float) -> int:
        return max(0, round((perf_counter() - started) * 1000))

    def _evidence_keys(self, evidence_refs: list[EvidenceRef]) -> list[str]:
        keys = [
            f"{evidence.source_type}:{evidence.source_id}"
            if evidence.source_id
            else evidence.source_type
            for evidence in evidence_refs
        ]
        return list(dict.fromkeys(keys))
