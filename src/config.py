from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from typing import Optional, List

class Settings(BaseSettings):
    # --- SECRETOS (Vendrán de Secret Manager en Cloud Run o .env en local) ---
    bot_token: str = Field(alias="BOT_TOKEN")
    openai_api_key: str = Field(alias="OPENAI_API_KEY")

    # --- CONFIGURACIÓN HARDCODED (Valores por defecto del proyecto) ---
    master_admin_id: int = Field(default=6901550600, alias="MASTER_ADMIN_ID")
    authorized_users: str = Field(default="6901550600", alias="AUTHORIZED_USERS")
    openai_model: str = Field(default="gpt-5.4", alias="OPENAI_MODEL")
    gcp_project_id: str = Field(default="mi-facturador-bot-01", alias="GCP_PROJECT_ID")
    gcs_bucket_name: str = Field(default="facturas-storage-mi-facturador-bot-01", alias="GCS_BUCKET_NAME")

    # --- AJUSTES INTERNOS ---
    temp_dir: str = "/tmp/temp_tickets"
    processed_dir: str = "facturas_procesadas"
    webhook_url: str = Field(default="", alias="WEBHOOK_URL")
    webhook_path: str = Field(default="/webhook", alias="WEBHOOK_PATH")
    port: int = Field(default=8080, alias="PORT")
    web_server_host: str = Field(default="0.0.0.0", alias="WEB_SERVER_HOST")
    
    @property
    def allowed_users_list(self) -> List[int]:
        return [int(u.strip()) for u in self.authorized_users.split(",") if u.strip().isdigit()]

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

config = Settings()
