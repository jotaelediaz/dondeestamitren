# app/config.py
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # --- API / Renfe ---
    RENFE_VEHICLE_POSITIONS_PB_URL: str | None = None
    RENFE_VEHICLE_POSITIONS_JSON_URL: str | None = None
    RENFE_TRIP_UPDATES_PB_URL: str | None = None
    RENFE_TRIP_UPDATES_JSON_URL: str | None = None
    ENABLE_TRIP_UPDATES_POLL: bool = False
    TRIP_UPDATES_POLL_SECONDS: int | None = None
    RENFE_HTTP_TIMEOUT: float = 7.0
    POLL_SECONDS: int = 8
    POLL_JITTER_S: int = 0

    # --- GTFS ---
    GTFS_RAW_DIR: str = "app/data/gtfs"
    GTFS_DELIMITER: str = ","
    GTFS_ENCODING: str = "utf-8"
    ROUTE_STATIONS_CSV: str = "app/data/derived/route_stations.csv"

    GTFS_STOPS_CSV: str | None = "app/data/gtfs/stops.txt"
    GTFS_STOPS_BY_NUCLEUS: dict[str, str] | None = None

    # --- Nuclei ---
    NUCLEI_DATA_CSV: str = "app/data/nucleos_data.csv"
    NUCLEI_MAP_CSV: str | None = "app/data/nucleos_map.csv"

    # --- Routes Parity calibration ---
    PARITY_INPUTS_GLOB: str = "app/data/raw/vehicle_positions/**/*.json*"
    PARITY_OUT_JSON: str = "app/data/derived/parity_map.json"
    PARITY_OVERRIDES_PATH: str | None = "app/data/custom/parity_overrides.json"

    # --- Local Additional Data ---
    MADRID_STATIONS_CSV: str | None = "app/data/custom/listado-estaciones-cercanias-madrid.csv"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )


settings = Settings()
