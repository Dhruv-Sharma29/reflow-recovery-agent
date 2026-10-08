"""Typed dashboard response projections, separate from request orchestration."""
from pydantic import BaseModel, Field
from app.audit.result import AuditRecord

class DashboardResponse(BaseModel):
    """Flattened, frontend-friendly view of a pipeline result.

    Every field is a direct projection from the pipeline result.
    No policy logic is applied here.
    """

    payment_id: str = Field(..., description="Razorpay payment ID")
    event_id: str = Field(..., description="Unique event identifier")

    # Classification
    failure_category: str | None = Field(
        default=None, description="Classified failure category"
    )
    classification_reason: str | None = Field(
        default=None, description="Classifier reason"
    )

    # AI recommendation (advisory; never authorization)
    recommendation_success: bool | None = Field(default=None)
    revenue_at_risk: bool | None = Field(default=None)
    risk_score: float | None = Field(default=None, ge=0.0, le=1.0)
    ai_suggested_cause: str | None = Field(default=None)
    ai_suggested_action: str | None = Field(default=None)
    ai_confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    recommendation_status: str | None = Field(default=None)
    recommendation_reason: str | None = Field(default=None)
    recommendation_model: str | None = Field(default=None)
    recommendation_latency_ms: int | None = Field(default=None, ge=0)
    recommendation_is_fallback: bool | None = Field(default=None)
    recommendation_fallback_reason: str | None = Field(default=None)

    # Policy (projected as-is from the pipeline)
    policy_action: str | None = Field(
        default=None, description="Policy-prescribed action"
    )
    policy_reason: str | None = Field(
        default=None, description="Policy decision reason"
    )
    automatic_recovery_allowed: bool | None = Field(
        default=None, description="Whether automatic recovery was authorized by policy"
    )

    # Reasoning
    reasoning_recommendation: str | None = Field(
        default=None, description="Model recommendation text"
    )
    reasoning_explanation: str | None = Field(
        default=None, description="Model explanation text"
    )
    reasoning_success: bool | None = Field(
        default=None, description="Whether the reasoning layer succeeded"
    )
    reasoning_is_fallback: bool | None = Field(
        default=None,
        description="True when the deterministic fallback produced the text",
    )
    reasoning_model: str | None = Field(
        default=None, description="Model that produced the explanation"
    )
    reasoning_latency_ms: int | None = Field(default=None, ge=0)
    root_cause_plain: str | None = Field(
        default=None, description="Plain-language root cause"
    )
    why_appropriate: str | None = Field(
        default=None, description="Why the policy action fits this failure"
    )
    customer_message: str | None = Field(
        default=None, description="Suggested customer-facing copy"
    )
    escalation_summary: str | None = Field(
        default=None, description="Summary for a human reviewer"
    )
    reasoning_fallback_reason: str | None = Field(
        default=None,
        description="Categorized reason the reasoning layer fell back (presentation-only)",
    )
    reasoning_from_cache: bool | None = Field(
        default=None,
        description="True if the reasoning explanation was served from cache",
    )

    # Execution
    execution_status: str | None = Field(
        default=None, description="Executor outcome status"
    )
    execution_reason: str | None = Field(
        default=None, description="Executor outcome reason"
    )
    payment_status: str | None = Field(
        default=None,
        description="Simulated gateway payment status (captured/failed/not_attempted)",
    )
    amount_recovered: int | None = Field(
        default=None, description="Amount actually recovered, in paise"
    )
    simulated: bool | None = Field(
        default=None,
        description="True when no real gateway was contacted (always true today)",
    )

    # Escalation
    escalation_status: str | None = Field(
        default=None, description="Escalation status"
    )
    escalation_reason: str | None = Field(
        default=None, description="Escalation reason"
    )
    escalation_severity: str | None = Field(
        default=None, description="Escalation severity"
    )

    # Final outcome
    final_outcome: str = Field(
        ..., description="Final pipeline outcome"
    )

    # Metadata
    timestamp: str = Field(..., description="ISO 8601 pipeline timestamp")
    amount: int | None = Field(default=None, description="Transaction amount in paise")
    attempt_number: int | None = Field(
        default=None, description="Attempt number"
    )
    error: str | None = Field(
        default=None, description="Pipeline error, if any"
    )


class AuditLogResponse(BaseModel):
    """List of audit records."""

    records: list[AuditRecord] = Field(
        default_factory=list, description="Audit log entries (this page)"
    )
    count: int = Field(default=0, description="Number of records in this page")
    total: int = Field(
        default=0, description="Total matching records, ignoring pagination"
    )



from app.pipeline.result import PipelineResult
from app.models.payment_event import FailedTransactionEvent

def _pipeline_to_response(result: PipelineResult, event: FailedTransactionEvent | None = None) -> DashboardResponse:
    """Project a PipelineResult into a DashboardResponse.

    No policy logic — pure field extraction.
    """
    amount = event.amount if event is not None else None
    attempt_number = event.attempt_number if event is not None else None

    return DashboardResponse(
        payment_id=result.payment_id,
        event_id=result.event_id,
        # Classification
        failure_category=(
            result.classification.category.value
            if result.classification is not None
            else None
        ),
        classification_reason=(
            result.classification.reason
            if result.classification is not None
            else None
        ),
        # AI recommendation
        recommendation_success=(
            result.recommendation.success
            if result.recommendation is not None
            else None
        ),
        revenue_at_risk=(
            result.recommendation.revenue_at_risk
            if result.recommendation is not None
            else None
        ),
        risk_score=(
            result.recommendation.risk_score
            if result.recommendation is not None
            else None
        ),
        ai_suggested_cause=(
            result.recommendation.suggested_cause.value
            if result.recommendation is not None
            and result.recommendation.suggested_cause is not None
            else None
        ),
        ai_suggested_action=(
            result.recommendation.suggested_action.value
            if result.recommendation is not None
            and result.recommendation.suggested_action is not None
            else None
        ),
        ai_confidence=(
            result.recommendation.confidence
            if result.recommendation is not None
            else None
        ),
        recommendation_status=(
            result.policy_decision.recommendation_status.value
            if result.policy_decision is not None
            else None
        ),
        recommendation_reason=(
            result.policy_decision.recommendation_reason
            if result.policy_decision is not None
            else None
        ),
        recommendation_model=(
            result.recommendation.model_id
            if result.recommendation is not None
            else None
        ),
        recommendation_latency_ms=(
            result.recommendation.latency_ms
            if result.recommendation is not None
            else None
        ),
        recommendation_is_fallback=(
            result.recommendation.is_fallback
            if result.recommendation is not None
            else None
        ),
        recommendation_fallback_reason=(
            result.recommendation.fallback_reason.value
            if result.recommendation is not None
            and result.recommendation.fallback_reason is not None
            else None
        ),
        # Policy
        policy_action=(
            result.policy_decision.action.value
            if result.policy_decision is not None
            else None
        ),
        policy_reason=(
            result.policy_decision.reason
            if result.policy_decision is not None
            else None
        ),
        automatic_recovery_allowed=(
            result.policy_decision.automatic_recovery_allowed
            if result.policy_decision is not None
            else None
        ),
        # Reasoning
        reasoning_recommendation=(
            result.reasoning.recommendation
            if result.reasoning is not None
            else None
        ),
        reasoning_explanation=(
            result.reasoning.explanation
            if result.reasoning is not None
            else None
        ),
        reasoning_success=(
            result.reasoning.success
            if result.reasoning is not None
            else None
        ),
        reasoning_is_fallback=(
            result.reasoning.is_fallback
            if result.reasoning is not None
            else None
        ),
        reasoning_model=(
            result.reasoning.model_id if result.reasoning is not None else None
        ),
        reasoning_latency_ms=(
            result.reasoning.latency_ms if result.reasoning is not None else None
        ),
        reasoning_from_cache=(
            result.reasoning.from_cache if result.reasoning is not None else None
        ),
        root_cause_plain=(
            result.reasoning.root_cause_plain
            if result.reasoning is not None
            else None
        ),
        why_appropriate=(
            result.reasoning.why_appropriate
            if result.reasoning is not None
            else None
        ),
        customer_message=(
            result.reasoning.customer_message
            if result.reasoning is not None
            else None
        ),
        escalation_summary=(
            result.reasoning.escalation_summary
            if result.reasoning is not None
            else None
        ),
        reasoning_fallback_reason=(
            result.reasoning.fallback_reason.value
            if result.reasoning is not None and result.reasoning.fallback_reason is not None
            else None
        ),
        # Execution
        execution_status=(
            result.execution.status.value
            if result.execution is not None
            else None
        ),
        execution_reason=(
            result.execution.reason
            if result.execution is not None
            else None
        ),
        payment_status=(
            result.execution.payment_status
            if result.execution is not None
            else None
        ),
        amount_recovered=(
            result.execution.amount_recovered
            if result.execution is not None
            else None
        ),
        simulated=(
            result.execution.simulated
            if result.execution is not None
            else None
        ),
        # Escalation
        escalation_status=(
            result.escalation.status.value
            if result.escalation is not None
            else None
        ),
        escalation_reason=(
            result.escalation.reason
            if result.escalation is not None
            else None
        ),
        escalation_severity=(
            result.escalation.severity.value
            if result.escalation is not None
            else None
        ),
        # Final outcome
        final_outcome=result.final_outcome.value,
        # Metadata
        timestamp=result.timestamp.isoformat(),
        amount=amount,
        attempt_number=attempt_number,
        error=result.error,
    )
