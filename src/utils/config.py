"""
Настройки конфигурации с использованием Pydantic v2.
"""
from typing import Optional
from pydantic import Field, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Настройки приложения, загружаемые из переменных окружения."""
    
    POSTGRES_URI: str = Field(..., validation_alias="POSTGRES_URI")
    SUPABASE_URL: Optional[str] = None
    SUPABASE_SERVICE_KEY: Optional[str] = None
    # Optional API key for public Sheets access (fallback when service account is not provided)
    SHEETS_API_KEY: Optional[str] = None
    SHEETS_SA_JSON: Optional[str] = None
    BITRIX_WEBHOOK: Optional[str] = None
    GOOGLE_ADS_TOKEN: Optional[str] = None
    YANDEX_DIRECT_TOKEN: Optional[str] = None
    META_TOKEN: Optional[str] = None
    YOUTUBE_KEY: Optional[str] = None
    ARCHIVE_PATH: str = Field(default="./archive")
    LOG_LEVEL: str = Field(default="INFO")
    # Connection pool sizing for asyncpg (small defaults to avoid exhausting hosted DB limits)
    DB_POOL_MIN: int = Field(default=1, validation_alias="DB_POOL_MIN")
    DB_POOL_MAX: int = Field(default=4, validation_alias="DB_POOL_MAX")
    SHEETS_SPREADSHEET_ID: Optional[str] = None
    SHEETS_RANGE: Optional[str] = None
    # ELT processing configuration
    BATCH_SIZE: int = Field(default=2000, validation_alias="BATCH_SIZE")
    TEST_LIMIT: int = Field(default=100, validation_alias="TEST_LIMIT")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )


settings = Settings()
