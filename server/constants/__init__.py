import os
from dotenv import load_dotenv

load_dotenv()

APPPORT: int = int(os.getenv("APP_SERVER_PORT", 8000))
APPHOST: str = os.getenv("APP_SERVER_HOST", "0.0.0.0")

AWS_ACCESS_KEY: str = os.getenv("AWS_ACCESS_KEY", "0.0.0.0")
AWS_SECRET_KEY: str = os.getenv("AWS_SECRET_KEY", "0.0.0.0")
REGION_NAME: str = os.getenv("AWS_REGION", "your-aws-region")
