import os
from dotenv import load_dotenv

load_dotenv()  # Load variables from .env file


DATABASE_URL = os.getenv("DATABASE_URL")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")