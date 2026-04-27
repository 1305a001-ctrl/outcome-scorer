from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Database
    aicore_db_url: str = ""

    # Horizons to score every signal at (in hours, comma-separated)
    horizons_hours: str = "4,24,168"  # 4h, 24h, 7d

    # Flat band — pct change within +-threshold counts as "flat"
    flat_threshold_pct: float = 0.005  # 0.5%

    # How long after a signal we wait before scoring (skip if horizon not yet elapsed)
    min_signal_age_hours: int = 4

    # Price feed APIs
    finnhub_key: str = ""
    binance_base_url: str = "https://api.binance.com"
    finnhub_base_url: str = "https://finnhub.io/api/v1"

    # Sentry
    sentry_dsn: str = ""

    @property
    def horizons(self) -> list[int]:
        return sorted({int(h.strip()) for h in self.horizons_hours.split(",") if h.strip()})


settings = Settings()
