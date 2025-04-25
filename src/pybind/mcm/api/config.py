from pydantic import BaseSettings

class Settings(BaseSettings):
    db_url: str = "postgresql+asyncpg://yugabyte:password@localhost:5433/your_db"

    class Config:
        env_file = ".env"  # You can load from .env file

# Singleton settings instance
settings = Settings()

def get_settings():
    return settings