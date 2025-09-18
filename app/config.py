# app/config.py
from __future__ import annotations

from pydantic import AnyHttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # --- App / Renfe ---
    RENFE_API_BASE: AnyHttpUrl | None = None
    REQUEST_TIMEOUT_S: float = 10.0
    POLL_SECONDS: int = 8

    # --- GTFS ---
    GTFS_RAW_DIR: str = "app/data/gtfs"
    GTFS_DELIMITER: str = ","
    GTFS_ENCODING: str = "utf-8"
    ROUTE_STATIONS_CSV: str = "app/data/derived/route_stations.csv"

    # --- Cercanías ---
    NUCLEI_DATA_CSV: str = "app/data/nucleos_data.csv"
    NUCLEI_MAP_CSV: str | None = "app/data/nucleos_map.csv"
    MADRID_STATIONS_CSV: str | None = "app/data/custom/listado-estaciones-cercanias-madrid.csv"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
