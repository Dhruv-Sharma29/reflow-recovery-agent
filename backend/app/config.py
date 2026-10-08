from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    razorpay_key_id: str = ""
    razorpay_key_secret: str = ""
    nim_api_key: str = ""
    nim_base_url: str = "https://integrate.api.nvidia.com/v1"
    nim_model: str = "nvidia/nemotron-3-nano-omni-30b-a3b-reasoning"
    database_url: str = "sqlite:///./recovery.db"
    auto_recovery_amount_limit: int = 500000
    executor_mode: str = "mock"  # "mock" | "razorpay_test"
    # Set in the Razorpay dashboard when creating the webhook. This is NOT
    # razorpay_key_secret — a different value signs webhook bodies. Empty
    # means the webhook endpoint refuses every request rather than trusting
    # unsigned input.
    razorpay_webhook_secret: str = ""
    # How sure the advisor must be before its action choice is taken over the
    # policy default. The A/B showed an ungated advisor losing ground, so the
    # bar exists to make it earn the override rather than merely have an
    # opinion. Raising this toward 1.0 approaches pure deterministic policy.
    model_action_choice_min_confidence: float = 0.7
    # Retries for a transient NIM failure (429/5xx/timeout). Measured against
    # the live endpoint, roughly a third of calls at concurrency 5 were shed
    # with a 503. Set to 0 in tests so simulated failures do not sleep.
    nim_max_retries: int = 2
    environment: str = "development"
    api_secret_key: str = ""
    # Explicit local-development opt-in; ignored outside development.
    allow_unauthenticated_development: bool = False
    # Comma-separated list of browser origins allowed to call the API.
    # Wildcards are intentionally avoided: a specific allow-list is required
    # for credentialed CORS requests to work at all.
    # 4173 is the port playwright.config.ts serves the e2e run on. Without it
    # every API call from that suite is blocked, so data-dependent UI never
    # renders and the contrast checks silently skip it.
    cors_allow_origins: str = (
        "http://localhost:5173,http://localhost:3000,http://localhost:4173"
    )

    @property
    def cors_origins_list(self) -> list[str]:
        return [o.strip() for o in self.cors_allow_origins.split(",") if o.strip()]


settings = Settings()
