from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    db_url: str = "postgresql+asyncpg://anmolb:mcm@localhost:5432/mcm"

    class Config:
        env_file = ".env"  # You can load from .env file

# Singleton settings instance
settings = Settings()

def get_settings():
    return settings
