from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    APP_NAME: str = "AI Data Workflow"
    DATABASE_URL: str
    # DATABASE_URL: str = "postgresql+psycopg://postgres:gai3509@localhost:5432/aidata"
    ARTIFACT_DIR: str

    # LLM (optional) add api key .env file.
    OPENAI_API_KEY: str | None = None
    OPENAI_MODEL: str = "gpt-4o-mini"


settings = Settings()