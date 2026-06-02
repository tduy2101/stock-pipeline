from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parents[1] / ".env", override=False)

DATABASE_URL: str = os.environ.get(
    "DATABASE_URL",
    "postgresql://stock:stock@localhost:55432/stock_pipeline",
)

DEFAULT_PAGE_SIZE: int = 100
MAX_PAGE_SIZE: int = 500

GOLD_SCHEMA: str = "gold"
