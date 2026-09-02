from typing import List, Optional
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # ===== API =====
    API_TITLE: str = "CODUKU API"
    API_VERSION: str = "2.0.0"
    API_PREFIX: str = "/api/v1"
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    ENVIRONMENT: str = "development"
    DEBUG: bool = True

    # ===== JWT =====
    JWT_SECRET: str = "local-dev-secret-key-12345"
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRATION_HOURS: int = 720

    # ===== Supabase (optional for local dev) =====
    SUPABASE_URL: Optional[str] = None
    SUPABASE_ANON_KEY: Optional[str] = None
    SUPABASE_SERVICE_ROLE_KEY: Optional[str] = None

    # ===== Redis (optional for local dev) =====
    UPSTASH_REDIS_URL: Optional[str] = None
    REDIS_NAMESPACE: str = "coduku"

    # ===== Judge0 =====
    JUDGE0_API_URL: str = "http://localhost:2358"
    JUDGE0_TIMEOUT: int = 10

    # ===== CORS =====
    CORS_ORIGINS: List[str] = [
        "http://localhost:3000",
        "http://localhost:3001",
        "https://coduku-frontend.vercel.app",
    ]

    # ===== DB =====
    MONGODB_URL: Optional[str] = None
    
    # ===== OpenAI =====
    OPENAI_API_KEY: str = "sk-test-default-key-for-development"

    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "allow"  # Allow extra fields from .env


settings = Settings()