from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from typing import Optional, List

class Settings(BaseSettings):
    # Telegram
    bot_token: str = Field(alias="BOT_TOKEN")
    master_admin_id: int = Field(default=0, alias="MASTER_ADMIN_ID")
    authorized_users: str = Field(default="", alias="AUTHORIZED_USERS")

    # OpenAI / LLM
    openai_api_key: str = Field(alias="OPENAI_API_KEY")
    openai_model: str = Field(default="gpt-4o-mini", alias="OPENAI_MODEL")

    # PostgreSQL
    postgres_db: str = Field(default="facturas_erp", alias="POSTGRES_DB")
    postgres_user: str = Field(default="erp_user", alias="POSTGRES_USER")
    postgres_password: str = Field(default="", alias="POSTGRES_PASSWORD")
    postgres_host: str = Field(default="localhost", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")

    # App Settings
    temp_dir: str = "temp_tickets"
    processed_dir: str = "facturas_procesadas"
    
    @property
    def allowed_users_list(self) -> List[int]:
        return [int(u.strip()) for u in self.authorized_users.split(",") if u.strip().isdigit()]

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

config = Settings()
