from pydantic import AnyHttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    RENFE_API_BASE: AnyHttpUrl | None = None
    REQUEST_TIMEOUT_S: float = 10.0
    POLL_SECONDS: int = 8

    GTFS_RAW_DIR: str = "app/data/gtfs/raw"
    GTFS_DELIMITER: str = ","
    GTFS_ENCODING: str = "utf-8"

    ROUTE_STATIONS_CSV: str = "app/data/derived/route_stations.csv"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
