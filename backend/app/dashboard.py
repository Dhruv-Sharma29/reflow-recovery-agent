"""Dashboard API router (TASK-009).

Read-only presentation layer for the Recovery Agent pipeline.
This router does NOT:
- implement or duplicate policy logic
- call Razorpay directly
- call NIM / Nemotron directly
- authorize recovery independently
- override any backend policy decision

All recovery decisions come from the existing pipeline components.
"""

from __future__ import annotations

import itertools
import json
import logging
import queue
import random
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from collections.abc import Callable, Iterator
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request, Depends
from fastapi.responses import StreamingResponse
from pydantic import ValidationError
from app.dashboard_models import DashboardResponse, AuditLogResponse, _pipeline_to_response

from app.config import settings

from app.audit.result import AuditOutcome, AuditRecord
from app.audit.store import AuditLogger
from app.classifier.engine import FailureClassifier
from app.escalation.handler import EscalationHandler
from app.executor.mock import MockExecutor
from app.models.payment_event import FailedTransactionEvent
from app.evaluation.harness import classify_funnel_stage
from app.ingestion.generator import generate_dataset
from app.outreach import SimulatedOutreachDispatcher
from app.persistence.store import RecoveryStateStore
from app.scheduler import run_due_jobs
from app.pipeline.engine import RecoveryPipeline
from app.razorpay.health import check_credentials
from app.razorpay.webhook import (
    WebhookRejected,
    parse_failed_payment,
    verify_signature,
)
from app.recommendation.engine import RecoveryRecommender
from app.pipeline.result import PipelineResult
from app.policy.engine import RecoveryPolicyEngine
from app.reasoning.engine import RecoveryReasoner

from app.auth import get_api_key

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/dashboard",
    tags=["dashboard"],
    dependencies=[Depends(get_api_key)]
)

# ---------------------------------------------------------------------------
# Shared pipeline instance (in-memory SQLite, mock executor)
# ---------------------------------------------------------------------------

# Persistent audit store: honors DATABASE_URL (defaults to a file DB) so
# records survive a backend restart instead of vanishing with an in-memory DB.
_audit_logger = AuditLogger(settings.database_url)
# Durable idempotency + attempt history, shared with the executor so recovery
# state survives a backend restart.
_state_store = RecoveryStateStore(settings.database_url)


def _make_executor(state_store=None):
    """Select executor based on EXECUTOR_MODE.

    Defaults to MockExecutor for offline / demo use.  RazorpayTestExecutor
    is only loaded when explicitly requested via EXECUTOR_MODE=razorpay_test.
    """
    if settings.executor_mode == "razorpay_test":
        from app.razorpay.executor import RazorpayTestExecutor
        return RazorpayTestExecutor(state_store=state_store)
    # realistic_capture on: an authorised action does not always land, so
    # the reported recovery rate is believable rather than a flat 100%.
    return MockExecutor(state_store=state_store, realistic_capture=True)


# Request pipeline: carries the state store, so an action with a policy
# cooldown is persisted as a scheduled job and reported pending.
_pipeline = RecoveryPipeline(
    classifier=FailureClassifier(),
    policy_engine=RecoveryPolicyEngine(),
    recommender=RecoveryRecommender(),
    reasoner=RecoveryReasoner(),
    executor=_make_executor(state_store=_state_store),
    escalation_handler=EscalationHandler(),
    audit_logger=_audit_logger,
    state_store=_state_store,
    outreach=SimulatedOutreachDispatcher(),
)

# Batch pipeline: identical to the request pipeline except its reasoner has
# no API key, so it never makes a live LLM call. Reasoning is advisory only —
# it cannot change an outcome or an amount — so skipping it leaves every
# batch metric identical while keeping a 500-event run fast. Opt back in per
# request with ?explain=true.
_fast_pipeline = RecoveryPipeline(
    classifier=FailureClassifier(),
    policy_engine=RecoveryPolicyEngine(),
    recommender=RecoveryRecommender(nim_api_key=""),
    reasoner=RecoveryReasoner(nim_api_key=""),
    executor=_make_executor(state_store=_state_store),
    escalation_handler=EscalationHandler(),
    audit_logger=_audit_logger,
    state_store=_state_store,
    outreach=SimulatedOutreachDispatcher(),
)

# Control arm for A/B: identical to the request pipeline except the advisor
# is not allowed to choose the action, so the two arms differ by exactly one
# variable.
_control_pipeline = RecoveryPipeline(
    classifier=FailureClassifier(),
    policy_engine=RecoveryPolicyEngine(),
    reasoner=RecoveryReasoner(),
    executor=_make_executor(_state_store),
    escalation_handler=EscalationHandler(),
    audit_logger=_audit_logger,
    state_store=_state_store,
    outreach=SimulatedOutreachDispatcher(),
    recommender=RecoveryRecommender(),
    allow_model_action_choice=False,
)

def _arm_pipeline(
    state_store: RecoveryStateStore, *, allow_model_action_choice: bool
) -> RecoveryPipeline:
    """One arm of the A/B, bound to its own isolated state.

    Everything is shared with the live pipeline except the store and the one
    variable under test, so a difference between arms can only come from that
    variable. The audit logger is shared deliberately: the run should still be
    recorded, and audit writes cannot affect a recovery outcome.
    """
    return RecoveryPipeline(
        classifier=FailureClassifier(),
        policy_engine=RecoveryPolicyEngine(),
        recommender=RecoveryRecommender(),
        reasoner=RecoveryReasoner(),
        executor=_make_executor(state_store=state_store),
        escalation_handler=EscalationHandler(),
        audit_logger=_audit_logger,
        state_store=state_store,
        outreach=SimulatedOutreachDispatcher(),
        allow_model_action_choice=allow_model_action_choice,
    )


# Worker pipeline: deliberately has NO state store, so a due job actually
# executes instead of rescheduling itself.
_worker_pipeline = RecoveryPipeline(
    classifier=FailureClassifier(),
    policy_engine=RecoveryPolicyEngine(),
    recommender=RecoveryRecommender(),
    reasoner=RecoveryReasoner(),
    executor=_make_executor(state_store=_state_store),
    escalation_handler=EscalationHandler(),
    audit_logger=_audit_logger,
)


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/process", response_model=DashboardResponse)
def process_payment(event: FailedTransactionEvent) -> DashboardResponse:
    """Run a payment event through the recovery pipeline.

    The pipeline makes all decisions. This endpoint only projects
    the result for display.
    """
    try:
        result = _pipeline.process(event)
    except Exception as exc:
        logger.error("Pipeline processing failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"Pipeline error: {exc}") from exc

    return _pipeline_to_response(result, event)


@router.post("/run-scheduled")
def run_scheduled(
    now: str | None = Query(
        default=None,
        description=(
            "ISO-8601 evaluation time. Lets a demo run a 24h cooldown "
            "without waiting. Defaults to the current time."
        ),
    ),
) -> dict[str, Any]:
    """Execute every scheduled retry whose cooldown has elapsed."""
    if now is None:
        evaluated_at = datetime.now(timezone.utc)
    else:
        try:
            evaluated_at = datetime.fromisoformat(now)
        except ValueError as exc:
            raise HTTPException(
                status_code=422, detail=f"Invalid 'now' timestamp: {now}"
            ) from exc
        if evaluated_at.tzinfo is None:
            evaluated_at = evaluated_at.replace(tzinfo=timezone.utc)

    try:
        report = run_due_jobs(_state_store, _worker_pipeline, now=evaluated_at)
    except Exception as exc:
        logger.error("Scheduler run failed: %s", exc)
        raise HTTPException(
            status_code=500, detail=f"Scheduler error: {exc}"
        ) from exc

    payload = report.as_dict()
    payload["evaluated_at"] = evaluated_at.isoformat()
    return payload


def _timing_summary(delays: list[int]) -> dict[str, Any]:
    """How fast the money came back, not just how much.

    Median is reported alongside the mean because a single 72h receivable
    reminder would otherwise dominate a batch of instant retries.
    """
    if not delays:
        return {"recovered_count": 0, "median_seconds": None, "max_seconds": None,
                "instant_count": 0}
    ordered = sorted(delays)
    mid = len(ordered) // 2
    median = (
        ordered[mid]
        if len(ordered) % 2
        else (ordered[mid - 1] + ordered[mid]) // 2
    )
    return {
        "recovered_count": len(ordered),
        "median_seconds": median,
        "max_seconds": ordered[-1],
        "instant_count": sum(1 for d in ordered if d == 0),
    }


def _fresh_batch(count: int, seed: int | None = None) -> list[FailedTransactionEvent]:
    """Generate ``count`` synthetic failed events with brand-new identifiers.

    Fresh ids on every run are what make the batch runner repeatable: runs
    accumulate in the audit log instead of colliding on idempotency. The
    seed is random by default so repeated demo runs produce different
    numbers; ``evaluate.py`` keeps its fixed seed for reproducibility.
    """
    pool_seed = seed if seed is not None else random.randrange(1_000_000)
    pool = generate_dataset(seed=pool_seed, total=100)

    events: list[FailedTransactionEvent] = []
    for i in range(count):
        template = pool[i % len(pool)]
        suffix = uuid.uuid4().hex[:12]
        events.append(
            template.model_copy(
                update={
                    "event_id": f"evt_batch_{suffix}",
                    "razorpay_payment_id": f"pay_batch_{suffix}",
                }
            )
        )
    return events


@router.post("/run-batch")
def run_batch(
    count: int = Query(default=25, ge=1, le=500, description="Events to process"),
    run_scheduler: bool = Query(
        default=True,
        description="Run deferred retries to completion before measuring",
    ),
    seed: int | None = Query(
        default=None, description="Fix the generator seed for a reproducible batch"
    ),
    explain: bool = Query(
        default=False,
        description=(
            "Call the live model for every event. Off by default: reasoning "
            "is advisory and cannot change any outcome, so a batch would pay "
            "one LLM round trip per event for text nobody reads."
        ),
    ),
) -> dict[str, Any]:
    """Process a fresh batch of synthetic failures and measure money recovered.

    Repeatable by design: every call generates new event ids, so runs
    accumulate rather than deduplicating against earlier ones.
    """
    return _execute_batch(
        count,
        seed,
        run_scheduler,
        explain,
        _pipeline if explain else _fast_pipeline,
    )


# A sentinel that cannot collide with a progress record, so the SSE drain loop
# can tell "batch finished" from "no case ready yet".
_STREAM_DONE = object()


@router.get("/run-batch/stream")
def run_batch_stream(
    count: int = Query(10, ge=1, le=200),
    seed: int | None = Query(None),
    run_scheduler: bool = Query(True),
    explain: bool = Query(False),
) -> StreamingResponse:
    """Stream a batch as server-sent events, one frame per case.

    Same work and same summary as ``/run-batch`` — the difference is when the
    caller learns about it. With ``explain=true`` each case costs an LLM round
    trip, so a batch that would otherwise be a long silence becomes a feed
    that fills in as the agent works through it.
    """
    frames: queue.Queue[Any] = queue.Queue()
    pipeline = _pipeline if explain else _fast_pipeline

    def _run() -> None:
        try:
            summary = _execute_batch(
                count, seed, run_scheduler, explain, pipeline, on_event=frames.put
            )
            frames.put(("summary", summary))
        except Exception as exc:  # noqa: BLE001
            logger.exception("Streaming batch failed: %s", exc)
            frames.put(("error", {"message": str(exc)}))
        finally:
            # Always released, so a failed batch closes the stream rather than
            # leaving the client waiting on a connection that will never speak.
            frames.put(_STREAM_DONE)

    worker = threading.Thread(target=_run, name="run-batch-stream", daemon=True)
    worker.start()

    def _frames() -> Iterator[str]:
        yield _sse("start", {"count": count, "seed": seed, "explain": explain})
        while True:
            frame = frames.get()
            if frame is _STREAM_DONE:
                break
            if isinstance(frame, tuple):
                name, payload = frame
                yield _sse(name, payload)
            else:
                yield _sse("case", frame)
        worker.join(timeout=5)

    return StreamingResponse(
        _frames(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            # Proxies that buffer would defeat the entire point of streaming.
            "X-Accel-Buffering": "no",
        },
    )


def _sse(event: str, data: Any) -> str:
    """Encode one server-sent event frame."""
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


# What acting wrongly costs, in paise. A stated model, not measured data —
# the same standing as the capture rates in the executor, and labelled as such
# in the response so nobody mistakes it for an invoice.
#
# Counting only gross recoveries makes any agent look good, because retrying
# everything always recovers something. These are the prices of the attempts
# that should not have been made.
_COST_PER_ISSUER_ATTEMPT = 200          # Rs 2  — gateway and issuer processing
_COST_PER_CUSTOMER_FRICTION = 500       # Rs 5  — chasing someone wrongly
_COST_PER_DOUBLE_CHARGE_RISK = 2000     # Rs 20 — a dispute a human must handle


def _restraint_cost(naive: dict[str, int]) -> dict[str, Any]:
    """Price the attempts the policy declined to make.

    "85 attempts avoided" is a count nobody can weigh. The same restraint
    stated in rupees is comparable against the money recovered, which is the
    only way to argue that stopping was worth more than chasing.
    """
    issuer = naive["extra_attempts"] * _COST_PER_ISSUER_ATTEMPT
    # Retrying past the cap is where a customer gets charged twice, and a
    # dispute costs far more than the retry that caused it.
    double_charge = naive["attempts_past_retry_cap"] * _COST_PER_DOUBLE_CHARGE_RISK
    # A blind retry on a cause nobody understood still reaches the customer.
    friction = (
        naive["blind_retries_on_unknown_cause"] + naive["non_retryable_retried"]
    ) * _COST_PER_CUSTOMER_FRICTION
    return {
        "cost_avoided": issuer + double_charge + friction,
        "cost_breakdown": {
            "issuer_attempts": issuer,
            "double_charge_exposure": double_charge,
            "customer_friction": friction,
        },
        "cost_model": {
            "per_issuer_attempt": _COST_PER_ISSUER_ATTEMPT,
            "per_customer_friction": _COST_PER_CUSTOMER_FRICTION,
            "per_double_charge_risk": _COST_PER_DOUBLE_CHARGE_RISK,
            "stated_not_measured": True,
        },
    }


def _progress_record(
    index: int,
    total: int,
    event: FailedTransactionEvent,
    result: PipelineResult | None,
) -> dict[str, Any]:
    """One line of the live feed: what the agent decided about one payment.

    Deliberately small — the full batch summary still arrives at the end, and
    a progress frame that duplicated it would just be a slower response.
    """
    decision = result.policy_decision if result is not None else None
    classification = result.classification if result is not None else None
    execution = result.execution if result is not None else None
    outcome = (
        getattr(result.final_outcome, "value", None) if result is not None else None
    )
    return {
        "index": index,
        "total": total,
        "payment_id": event.razorpay_payment_id,
        "amount": event.amount,
        "category": getattr(getattr(classification, "category", None), "value", None),
        "action": getattr(getattr(decision, "action", None), "value", None),
        "allowed": bool(decision.automatic_recovery_allowed) if decision else False,
        "escalation_reason": getattr(
            getattr(decision, "escalation_reason", None), "value", None
        ),
        "recovered": bool(execution is not None and execution.executed),
        "outcome": outcome,
    }


def _execute_batch(
    count: int,
    seed: int | None,
    run_scheduler: bool,
    explain: bool,
    pipeline: RecoveryPipeline,
    on_event: Callable[[dict[str, Any]], None] | None = None,
    state_store: RecoveryStateStore | None = None,
) -> dict[str, Any]:
    """Run one batch through the given pipeline and summarise it.

    ``on_event`` receives a small progress record as each case finishes, so a
    caller can stream the run. It fires from a worker thread.
    """
    started = datetime.now(timezone.utc)
    events = _fresh_batch(count, seed)

    attempted_amount = 0
    recoverable_amount = 0
    recovered_amount = 0
    reasoning_model_generated = 0
    reasoning_fallback = 0
    reasoning_customer_messages = 0
    reasoning_from_cache = 0
    reasoning_latencies: list[int] = []
    reasoning_prompt_version: str | None = None
    reasoning_schema_version: str | None = None
    recommendation_model_generated = 0
    recommendation_fallback = 0
    recommendation_risk_detected = 0
    recommendation_latencies: list[int] = []
    recommendation_prompt_version: str | None = None
    recommendation_statuses = {
        "accepted": 0,
        "constrained": 0,
        "rejected": 0,
        "unavailable": 0,
    }
    # Computed, never assumed: the reasoner copies policy_action_allowed
    # verbatim, so this must stay 0. A non-zero value would mean the safety
    # boundary had been breached, and is worth surfacing rather than hiding.
    reasoning_overrode_policy = 0
    # Times the advisor picked a different — but equally authorised —
    # action from the policy's permitted set. This is the AI's one
    # measurable effect on behaviour.
    model_chose_action = 0
    # A real contact attempt, not a synonym for "an action executed".
    outreach_attempted = 0
    outreach_delivered = 0
    # Delay per recovered payment, in seconds. Inline actions land now; a
    # deferred retry lands after its cooldown.
    recovery_delays: list[int] = []
    # What a naive "retry everything" agent would have done with the cases
    # this one refused. Derived from the policy's own refusal reason, so it
    # is a restatement of real decisions rather than a guess.
    naive = {
        "extra_attempts": 0,
        "amount_chased_past_cap": 0,
        "attempts_past_retry_cap": 0,
        "blind_retries_on_unknown_cause": 0,
        "non_retryable_retried": 0,
    }
    # Why the advisor did or did not influence this run. Without these the
    # only available explanation is "no choices were made", which blames the
    # model for what is usually a lack of opportunity.
    advisor = {
        "events_with_alternatives": 0,
        "model_answers": 0,
        "proposed_change": 0,
        "blocked_by_confidence": 0,
        "applied": 0,
    }
    audit_ids: list[str] = []
    outcomes: dict[str, int] = {}
    funnel = {"raw": 0, "needed_signal": 0, "contacted": 0, "confirmed_recovered": 0}
    by_category: dict[str, dict[str, Any]] = {}

    # Recovery actions breakdown — executor-level, distinct from the funnel.
    ra_retries_attempted = 0
    ra_payments_recovered = 0
    ra_payments_pending = 0
    ra_payments_escalated = 0
    ra_execution_failed = 0

    # Process events in parallel via the pipeline's thread pool
    progress_index = itertools.count(1)

    def _report(
        event: FailedTransactionEvent, result: PipelineResult | None
    ) -> None:
        """Emit one case's outcome the moment the pipeline finishes with it."""
        if on_event is None:
            return
        on_event(_progress_record(next(progress_index), len(events), event, result))

    results = pipeline.process_batch(
        events, max_workers=5, on_result=_report if on_event else None
    )

    for event, result in zip(events, results):
        if result is None:
            # Exception was logged by process_batch
            continue

        decision = result.policy_decision
        if decision is not None:
            permitted = decision.permitted_actions or []
            # The ceiling on what any advisor could change. One permitted
            # action means there was nothing to decide.
            if len(permitted) > 1:
                advisor["events_with_alternatives"] += 1
            rec = result.recommendation
            if rec is not None and rec.success:
                advisor["model_answers"] += 1
            if result.action_source == "model":
                advisor["applied"] += 1
            elif (
                rec is not None
                and rec.suggested_action is not None
                and rec.suggested_action in permitted
                and rec.suggested_action != decision.action
                and decision.automatic_recovery_allowed
            ):
                # action_source is "policy", so decision.action is still the
                # default: the advisor wanted a change and did not get it.
                advisor["proposed_change"] += 1
                if rec.confidence < settings.model_action_choice_min_confidence:
                    advisor["blocked_by_confidence"] += 1

        if decision is not None and not decision.automatic_recovery_allowed:
            # Naive would have retried this anyway.
            naive["extra_attempts"] += 1
            reason = getattr(decision.escalation_reason, "value", None)
            amount_here = event.amount or 0
            if reason == "amount_exceeds_limit":
                naive["amount_chased_past_cap"] += amount_here
            elif reason in ("retry_limit_exhausted", "global_attempt_cap"):
                naive["attempts_past_retry_cap"] += 1
            elif reason == "unknown_failure":
                naive["blind_retries_on_unknown_cause"] += 1
            elif reason == "non_retryable_failure":
                naive["non_retryable_retried"] += 1

        if result.outreach is not None and result.outreach.attempted:
            outreach_attempted += 1
            if result.outreach.delivered:
                outreach_delivered += 1

        if getattr(result, "action_source", "policy") == "model":
            model_chose_action += 1

        attempted_amount += event.amount or 0
        # What policy actually authorised chasing. Recovered/recoverable
        # measures the agent; recovered/attempted is dominated by the cases
        # it correctly refused to touch.
        if (
            result.policy_decision is not None
            and result.policy_decision.automatic_recovery_allowed
        ):
            recoverable_amount += event.amount or 0
        outcome = result.final_outcome.value
        outcomes[outcome] = outcomes.get(outcome, 0) + 1
        if result.audit_write is not None and result.audit_write.record is not None:
            audit_ids.append(result.audit_write.record.audit_id)

        if result.recommendation is not None:
            recommendation = result.recommendation
            if recommendation.is_fallback:
                recommendation_fallback += 1
            else:
                recommendation_model_generated += 1
            if recommendation.revenue_at_risk:
                recommendation_risk_detected += 1
            if recommendation.latency_ms is not None:
                recommendation_latencies.append(recommendation.latency_ms)
            if recommendation.prompt_version and recommendation_prompt_version is None:
                recommendation_prompt_version = recommendation.prompt_version
            status = (
                result.policy_decision.recommendation_status.value
                if result.policy_decision is not None
                else "unavailable"
            )
            recommendation_statuses[status] = recommendation_statuses.get(status, 0) + 1

        if result.reasoning is not None:
            if result.reasoning.is_fallback:
                reasoning_fallback += 1
            else:
                reasoning_model_generated += 1
                if result.reasoning.from_cache:
                    reasoning_from_cache += 1
            if result.reasoning.latency_ms is not None:
                reasoning_latencies.append(result.reasoning.latency_ms)
            if result.reasoning.prompt_version and reasoning_prompt_version is None:
                reasoning_prompt_version = result.reasoning.prompt_version
            if result.reasoning.schema_version and reasoning_schema_version is None:
                reasoning_schema_version = result.reasoning.schema_version
            if result.reasoning.customer_message:
                reasoning_customer_messages += 1
            if (
                result.policy_decision is not None
                and result.reasoning.policy_action_allowed
                != result.policy_decision.automatic_recovery_allowed
            ):
                reasoning_overrode_policy += 1

        execution = result.execution
        got = (
            execution.amount_recovered
            if execution is not None and isinstance(execution.amount_recovered, int)
            else 0
        )
        recovered_amount += got
        if got > 0 and execution is not None:
            recovery_delays.append(
                int(getattr(execution, "recovery_delay_seconds", 0) or 0)
            )

        # Recovery actions counters — derived from outcome + execution.
        if execution is not None and execution.executed:
            ra_retries_attempted += 1
        if outcome == "recovered":
            ra_payments_recovered += 1
        elif outcome == "pending":
            ra_payments_pending += 1
        elif outcome == "escalated":
            ra_payments_escalated += 1
        elif outcome == "execution_failed":
            ra_execution_failed += 1

        category = (
            result.classification.category.value
            if result.classification is not None
            else "unknown"
        )
        bucket = by_category.setdefault(
            category,
            {"count": 0, "recovered_count": 0, "attempted_amount": 0,
             "recovered_amount": 0, "recovery_rate_amount": 0.0},
        )
        bucket["count"] += 1
        bucket["attempted_amount"] += event.amount or 0
        bucket["recovered_amount"] += got
        if got > 0:
            bucket["recovered_count"] += 1

        stages = classify_funnel_stage(
            automatic_recovery_allowed=bool(
                result.policy_decision is not None
                and result.policy_decision.automatic_recovery_allowed
            ),
            escalated=outcome == "escalated",
            executed=bool(execution is not None and execution.executed is True),
            recovered_amount=got,
        )
        for stage, hit in stages.items():
            funnel[stage] += int(hit)

    # Deferred retries only move money once the worker runs them.
    scheduler_summary: dict[str, Any] | None = None
    if run_scheduler:
        # Far enough ahead that every cooldown in this batch has elapsed.
        horizon = datetime.now(timezone.utc) + timedelta(days=365)
        # Whichever store this run owns. The A/B supplies an isolated one:
        # sweeping the shared queue would hand one arm every deferred retry
        # left by an earlier batch and call it that arm's recovery.
        report = run_due_jobs(
            state_store if state_store is not None else _state_store,
            _worker_pipeline,
            now=horizon,
        )
        scheduler_summary = report.as_dict()
        recovered_amount += report.amount_recovered
        recovery_delays.extend(report.delays_seconds)
        funnel["contacted"] += report.recovered + report.failed
        funnel["confirmed_recovered"] += report.recovered
        # Attribute deferred recoveries to the scenario they came from, or
        # the per-scenario card would contradict the batch total.
        for cat, amount in report.amount_by_category.items():
            bucket = by_category.setdefault(
                cat,
                {"count": 0, "recovered_count": 0, "attempted_amount": 0,
                 "recovered_amount": 0, "recovery_rate_amount": 0.0},
            )
            bucket["recovered_amount"] += amount
        for cat, hits in report.count_by_category.items():
            by_category[cat]["recovered_count"] += hits
        if report.recovered:
            outcomes["recovered"] = outcomes.get("recovered", 0) + report.recovered
            outcomes["pending"] = max(
                0, outcomes.get("pending", 0) - report.recovered
            )
            # Reconcile recovery_actions with deferred retries that completed.
            ra_retries_attempted += report.recovered + report.failed
            ra_payments_recovered += report.recovered
            ra_payments_pending = max(0, ra_payments_pending - report.recovered)
            ra_execution_failed += report.failed

    for bucket in by_category.values():
        if bucket["attempted_amount"] > 0:
            bucket["recovery_rate_amount"] = (
                bucket["recovered_amount"] / bucket["attempted_amount"]
            )

    processed = sum(outcomes.values())
    return {
        "transactions_processed": processed,
        "total_attempted_amount": attempted_amount,
        "total_recoverable_amount": recoverable_amount,
        "total_recovered_amount": recovered_amount,
        "recovery_rate_by_amount": (
            recovered_amount / attempted_amount if attempted_amount else 0.0
        ),
        "recovery_rate_of_recoverable": (
            recovered_amount / recoverable_amount if recoverable_amount else 0.0
        ),
        "recovery_rate_by_count": (
            funnel["confirmed_recovered"] / processed if processed else 0.0
        ),
        "outcomes": outcomes,
        "funnel": funnel,
        "by_scenario": [
            {"scenario": name, **values} for name, values in sorted(by_category.items())
        ],
        "timing": _timing_summary(recovery_delays),
        "advisor": advisor,
        "restraint": {
            **naive,
            **_restraint_cost(naive),
            "note": (
                "What a retry-everything agent would have done with the cases "
                "this one refused. Each extra attempt is a real issuer hit and "
                "a chance to charge a customer twice."
            ),
        },
        "outreach": {
            "attempted": outreach_attempted,
            "delivered": outreach_delivered,
            "simulated": True,
        },
        "audit_ids": audit_ids,
        "reasoning": {
            "mode": "model" if explain else "skipped",
            "consultations": reasoning_model_generated + reasoning_fallback,
            "model_generated": reasoning_model_generated,
            "fallback": reasoning_fallback,
            "from_cache": reasoning_from_cache,
            "customer_messages": reasoning_customer_messages,
            "overrode_policy": reasoning_overrode_policy,
            "chose_action": model_chose_action,
            "model": settings.nim_model,
            "prompt_version": reasoning_prompt_version,
            "schema_version": reasoning_schema_version,
            "average_latency_ms": (sum(reasoning_latencies) / len(reasoning_latencies)) if reasoning_latencies else 0.0,
        },
        "recommendation": {
            "mode": "model" if explain else "skipped",
            "consultations": recommendation_model_generated + recommendation_fallback,
            "model_generated": recommendation_model_generated,
            "fallback": recommendation_fallback,
            "risk_detected": recommendation_risk_detected,
            "accepted": recommendation_statuses["accepted"],
            "constrained": recommendation_statuses["constrained"],
            "rejected": recommendation_statuses["rejected"],
            "unavailable": recommendation_statuses["unavailable"],
            "model": settings.nim_model,
            "prompt_version": recommendation_prompt_version,
            "average_latency_ms": (
                sum(recommendation_latencies) / len(recommendation_latencies)
                if recommendation_latencies
                else 0.0
            ),
        },
        "recovery_actions": {
            "retries_attempted": ra_retries_attempted,
            "payments_recovered": ra_payments_recovered,
            "payments_pending": ra_payments_pending,
            "payments_escalated": ra_payments_escalated,
            "execution_failed": ra_execution_failed,
        },
        "scheduler": scheduler_summary,
        "simulated": True,
        "duration_seconds": (
            datetime.now(timezone.utc) - started
        ).total_seconds(),
    }


@router.post("/webhook/razorpay")
async def razorpay_webhook(request: Request) -> dict[str, Any]:
    """Ingest a live ``payment.failed`` notification.

    The signature covers the raw bytes, so the body is verified before it is
    parsed. A payload this system should not act on — a different event, or a
    payment that has already settled — is acknowledged rather than rejected,
    because Razorpay retries anything that is not a 2xx and there is nothing
    to retry here.
    """
    raw = await request.body()
    try:
        verify_signature(raw, request.headers.get("X-Razorpay-Signature"))
        event = parse_failed_payment(raw)
    except WebhookRejected as exc:
        # 400, never 500: the payload is the problem, and a 5xx would make
        # Razorpay redeliver a request that can never succeed.
        logger.warning("Webhook rejected: %s", exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if event is None:
        return {"accepted": True, "processed": False, "reason": "no action warranted"}

    try:
        result = _pipeline.process(event)
    except Exception as exc:
        logger.error("Webhook pipeline processing failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"Pipeline error: {exc}") from exc

    return {
        "accepted": True,
        "processed": True,
        "payment_id": event.razorpay_payment_id,
        "action": getattr(
            getattr(result.policy_decision, "action", None), "value", None
        ),
        "automatic_recovery_allowed": bool(
            result.policy_decision is not None
            and result.policy_decision.automatic_recovery_allowed
        ),
        "audit_id": getattr(result.audit_write, "audit_id", None),
    }


@router.get("/razorpay-check")
def razorpay_check() -> dict[str, Any]:
    """Would a real recovery call reach Razorpay right now?

    Makes one authenticated, read-only request that creates nothing. Answers
    the question that bad credentials, no network, and a correctly-refused
    missing mandate would otherwise all answer the same way.

    Never returns the secret — only whether one is set, and a masked key id.
    """
    return check_credentials()


@router.get("/learned")
def learned_outcomes(
    min_observations: int = Query(
        3,
        ge=1,
        description=(
            "Withhold a rate until it rests on at least this many attempts. "
            "One observation is noise, not a lesson."
        ),
    ),
) -> dict[str, Any]:
    """What the agent has measured about its own recovery actions.

    Aggregated from the append-only audit log, so these are outcomes the
    system actually produced — not a hardcoded table. The same figures are
    fed to the advisor, which is what lets it choose an action on evidence.

    Empty on a fresh database: the agent has not done anything to learn from
    yet, and saying so is more honest than showing a made-up prior.
    """
    rows = _audit_logger.outcome_stats(min_observations=min_observations)
    return {
        "min_observations": min_observations,
        "rows": rows,
        "learned": bool(rows),
        "note": (
            "Measured from this system's own audit log, not configured. "
            "Run more batches to sharpen it."
        ),
    }


@router.get("/provider")
def provider_status() -> dict[str, Any]:
    """Which reasoning provider is configured.

    Never returns the API key itself — only whether one is present, so the
    UI can say "the agent is on" without having to process an event first.
    """
    key = settings.nim_api_key or ""
    return {
        "provider": "nvidia-nim",
        "model": settings.nim_model,
        "base_url": settings.nim_base_url,
        "configured": bool(key.strip()),
    }


@router.get("/risk")
def revenue_at_risk(
    limit: int = Query(default=5, ge=1, le=50, description="Rows per breakdown"),
) -> dict[str, Any]:
    """Revenue-at-risk rollups derived from the audit log.

    Read-only aggregation over records the pipeline already wrote — no
    reprocessing, no new decisions. "At risk" is the value of failed
    payments; "recovered" is what the executor actually captured.
    """
    records = _audit_logger.list_records()

    by_merchant: dict[str, dict[str, int]] = {}
    by_customer: dict[str, dict[str, int]] = {}
    subscription = {"count": 0, "at_risk_amount": 0, "mandate_issues": 0}
    total_at_risk = 0
    total_recovered = 0

    for r in records:
        amount = r.amount or 0
        recovered = r.amount_recovered or 0
        total_at_risk += amount
        total_recovered += recovered

        if r.merchant_id:
            m = by_merchant.setdefault(
                r.merchant_id,
                {"failures": 0, "at_risk_amount": 0, "recovered_amount": 0},
            )
            m["failures"] += 1
            m["at_risk_amount"] += amount
            m["recovered_amount"] += recovered

        if r.customer_ref:
            c = by_customer.setdefault(
                r.customer_ref, {"failures": 0, "at_risk_amount": 0}
            )
            c["failures"] += 1
            c["at_risk_amount"] += amount

        if r.transaction_type == "subscription":
            subscription["count"] += 1
            subscription["at_risk_amount"] += amount
            # An expired or paused mandate cannot be retried into success:
            # it needs the customer to re-authorize.
            if r.mandate_status in ("expired", "paused"):
                subscription["mandate_issues"] += 1

    merchants = sorted(
        (
            {"merchant_id": k, **v, "outstanding_amount": v["at_risk_amount"] - v["recovered_amount"]}
            for k, v in by_merchant.items()
        ),
        key=lambda m: m["outstanding_amount"],
        reverse=True,
    )[:limit]

    # Only customers who failed more than once are "repeat" failures.
    repeats = sorted(
        (
            {"customer_ref": k, **v}
            for k, v in by_customer.items()
            if v["failures"] > 1
        ),
        key=lambda c: (c["failures"], c["at_risk_amount"]),
        reverse=True,
    )[:limit]

    return {
        "total_at_risk_amount": total_at_risk,
        "total_recovered_amount": total_recovered,
        "outstanding_amount": total_at_risk - total_recovered,
        "records_considered": len(records),
        "by_merchant": merchants,
        "repeat_customers": repeats,
        "subscription_failures": subscription,
    }


@router.post("/run-ab")
def run_ab(
    count: int = Query(default=25, ge=1, le=200, description="Events per arm"),
    seed: int = Query(default=42, description="Same seed for both arms"),
) -> dict[str, Any]:
    """Measure what the advisor's action choice is actually worth.

    Runs the same seeded batch twice — once with the advisor allowed to pick
    among the policy's permitted actions, once forced to the policy default.
    Both arms use live reasoning; the only difference between them is who
    chose the action.
    """
    # Each arm gets its own throwaway state. An A/B is only a comparison if
    # the arms are independent: sharing the live store let whichever ran first
    # sweep every deferred retry pending from earlier batches and book the
    # money as its own, which produced differences the advisor never caused —
    # including recovery rates above 100%.
    control_store = RecoveryStateStore(database_url="sqlite:///:memory:")
    treatment_store = RecoveryStateStore(database_url="sqlite:///:memory:")
    try:
        # The arms are independent: each has its own state store and the
        # only experimental variable remains action choice. Running them in
        # parallel matters when live NIM is configured — otherwise the
        # second arm waits for a full batch of model calls and can make a
        # perfectly valid comparison look like a client timeout.
        with ThreadPoolExecutor(max_workers=2, thread_name_prefix="ab-arm") as executor:
            control_future = executor.submit(
                _execute_batch,
                count,
                seed,
                True,
                True,
                _arm_pipeline(control_store, allow_model_action_choice=False),
                state_store=control_store,
            )
            treatment_future = executor.submit(
                _execute_batch,
                count,
                seed,
                True,
                True,
                _arm_pipeline(treatment_store, allow_model_action_choice=True),
                state_store=treatment_store,
            )
            control = control_future.result()
            treatment = treatment_future.result()
    finally:
        control_store.close()
        treatment_store.close()

    def _median(arm: dict[str, Any]) -> int | None:
        return arm["timing"]["median_seconds"]

    choices = treatment["reasoning"].get("chose_action", 0)
    return {
        "count_per_arm": count,
        "seed": seed,
        "control": {
            "label": "policy default action",
            "recovered_amount": control["total_recovered_amount"],
            "recovery_rate_of_recoverable": control["recovery_rate_of_recoverable"],
            "median_seconds_to_recovery": _median(control),
        },
        "treatment": {
            "label": "advisor chose among permitted actions",
            "recovered_amount": treatment["total_recovered_amount"],
            "recovery_rate_of_recoverable": treatment["recovery_rate_of_recoverable"],
            "median_seconds_to_recovery": _median(treatment),
            "actions_chosen_by_model": choices,
        },
        "delta": {
            "recovered_amount": treatment["total_recovered_amount"]
            - control["total_recovered_amount"],
            "recovery_rate_of_recoverable": treatment["recovery_rate_of_recoverable"]
            - control["recovery_rate_of_recoverable"],
            "median_seconds_to_recovery": (
                None
                if _median(control) is None or _median(treatment) is None
                else _median(treatment) - _median(control)
            ),
        },
        # Conclusive means the comparison could actually run, not that the
        # advisor changed something. A model that answered, had real choices
        # available, and agreed with policy every time has produced a finding
        # — reporting that as "inconclusive" throws the finding away.
        "conclusive": bool(choices)
        or bool(
            treatment["advisor"]["model_answers"]
            and treatment["advisor"]["events_with_alternatives"]
        ),
        "advisor": treatment["advisor"],
        "note": _ab_note(choices, count, treatment["advisor"]),
    }


def _ab_note(choices: int, count: int, advisor: dict[str, Any]) -> str:
    """Explain the result, and when there is none, explain why.

    "The advisor made no action choices" is true but useless: it reads as a
    missing API key when the usual cause is that policy authorised exactly one
    action, leaving nothing to choose. Each branch below names the actual
    constraint so the reader knows what to change.
    """
    opportunities = advisor["events_with_alternatives"]

    if choices:
        return (
            f"The advisor changed the action on {choices} of {count} events "
            f"({opportunities} offered more than one permitted action)."
        )

    if opportunities == 0:
        return (
            f"Policy authorised exactly one action for all {count} events, so "
            "there was nothing for the advisor to choose. This is a property "
            "of the batch, not a missing model — a comparison needs causes "
            "with alternatives, such as bank declines or network errors."
        )

    if advisor["model_answers"] == 0:
        return (
            f"{opportunities} of {count} events offered a real choice, but no "
            "live model answers were available, so both arms used the policy "
            "default. Set NIM_API_KEY to compare."
        )

    if advisor["blocked_by_confidence"]:
        return (
            f"The advisor proposed a change on {advisor['proposed_change']} "
            f"events but {advisor['blocked_by_confidence']} fell below the "
            "confidence threshold, so policy's default stood. Lower "
            "MODEL_ACTION_CHOICE_MIN_CONFIDENCE to admit them."
        )

    return (
        f"The advisor answered on {advisor['model_answers']} events and agreed "
        f"with policy every time, across {opportunities} that offered a real "
        "choice. Agreement is a result, not a failure."
    )


@router.post("/reset")
def reset_state() -> dict[str, Any]:
    """Clear recovery state so a demo can re-run cleanly. Test-mode only.

    Deliberately does NOT delete audit records: the audit log is
    append-only, and that guarantee is the point of the system. Clearing
    idempotency, attempt history, and scheduled jobs is enough to make a
    fresh batch behave like a first run.
    """
    try:
        _state_store.clear()
    except Exception as exc:  # noqa: BLE001
        logger.error("Reset failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"Reset error: {exc}") from exc
    return {
        "cleared": True,
        "recovery_state_cleared": True,
        "audit_log_preserved": True,
        "note": "Audit history is append-only and is never deleted.",
    }


@router.get("/scheduled")
def list_scheduled(
    status: str | None = Query(
        default=None, description="Filter by job status (pending/done/failed)"
    ),
) -> dict[str, Any]:
    """Read-only view of scheduled retry jobs."""
    jobs = _state_store.list_jobs(status=status)
    # event_json is the full stored payload; omit it from the listing.
    trimmed = [{k: v for k, v in job.items() if k != "event_json"} for job in jobs]
    return {"jobs": trimmed, "count": len(trimmed)}


@router.get("/audit", response_model=AuditLogResponse)
def get_audit_log(
    limit: int | None = Query(
        default=None, ge=1, le=500,
        description="Max records to return; omit to return all",
    ),
    offset: int = Query(default=0, ge=0, description="Records to skip (pagination)"),
    outcome: str | None = Query(
        default=None, description="Filter by final_outcome (e.g. 'recovered')"
    ),
) -> AuditLogResponse:
    """Return audit records. Read-only.

    Supports pagination (``limit``/``offset``) and an optional
    ``final_outcome`` filter. Omitting ``limit`` returns all matching records
    for backward compatibility. ``count`` is the size of the returned page;
    ``total`` is the full matching count.
    """
    try:
        records = _audit_logger.list_records(limit=limit, offset=offset, outcome=outcome)
        total = _audit_logger.count_records(outcome=outcome)
    except Exception as exc:
        logger.error("Audit log retrieval failed: %s", exc)
        raise HTTPException(
            status_code=500, detail="Could not read from audit database"
        ) from exc

    return AuditLogResponse(records=records, count=len(records), total=total)


@router.get("/audit/export")
def export_audit_log():
    from fastapi.responses import PlainTextResponse

    try:
        csv_data = _audit_logger.export_csv()
        return PlainTextResponse(content=csv_data, media_type="text/csv")
    except Exception as exc:
        logger.error("Audit log export failed: %s", exc)
        raise HTTPException(status_code=500, detail="Could not export audit database") from exc

@router.get("/telemetry")
def get_telemetry() -> dict[str, Any]:
    """Expose operational metrics for the recovery agent."""
    try:
        records = _audit_logger.list_records(limit=None)

        total_records = len(records)
        recovered_count = sum(1 for r in records if r.final_outcome == AuditOutcome.RECOVERED)
        recovery_rate = (recovered_count / total_records) if total_records > 0 else 0.0

        fallback_count = sum(1 for r in records if r.reasoning and r.reasoning.is_fallback)
        total_reasoning = sum(1 for r in records if r.reasoning)
        fallback_rate = (fallback_count / total_reasoning) if total_reasoning > 0 else 0.0

        latencies = [r.reasoning.latency_ms for r in records if r.reasoning and r.reasoning.latency_ms is not None]
        avg_latency = (sum(latencies) / len(latencies)) if latencies else 0.0

        cache = getattr(_pipeline.reasoner, 'cache', None)
        cache_hits = cache.hits if cache else 0
        cache_misses = cache.misses if cache else 0
        total_cache = cache_hits + cache_misses
        cache_hit_rate = (cache_hits / total_cache) if total_cache > 0 else 0.0

        return {
            "recovery_rate": round(recovery_rate, 4),
            "fallback_rate": round(fallback_rate, 4),
            "cache_hit_rate": round(cache_hit_rate, 4),
            "average_latency_ms": round(avg_latency, 2),
            "total_processed": total_records,
            "total_recovered": recovered_count,
        }
    except Exception as exc:
        logger.error("Telemetry computation failed: %s", exc)
        raise HTTPException(status_code=500, detail="Could not compute telemetry") from exc
@router.post("/golden-path")
def run_golden_path() -> DashboardResponse:
    """Run a fixed synthetic event through the full pipeline.
    
    This exercises classification -> policy -> reasoner -> executor -> audit log
    with a guaranteed "insufficient funds" failure and an amount below the 
    auto-recovery cap, so the policy engine will allow execution.
    """
    event = FailedTransactionEvent(
        event_id=str(uuid.uuid4()),
        razorpay_payment_id=f"pay_golden_{uuid.uuid4().hex[:8]}",
        merchant_id="merchant_demo",
        customer_id="cust_demo",
        type="one_time",
        amount=settings.auto_recovery_amount_limit - 100,  # Below cap
        currency="INR",
        payment_method="upi",
        error_code="BAD_REQUEST_ERROR",
        error_description="Your payment could not be completed due to insufficient balance. Please ensure you have sufficient funds and try again.",
        failure_category="insufficient_funds",
        attempt_number=1,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )
    
    try:
        # We always want the reasoner to run for the golden path to show the full output.
        result = _pipeline.process(event)
    except Exception as exc:
        logger.error("Golden path failed: %s", exc)
        raise HTTPException(
            status_code=500, detail=f"Golden path error: {exc}"
        ) from exc
        
    return _pipeline_to_response(result, event)
