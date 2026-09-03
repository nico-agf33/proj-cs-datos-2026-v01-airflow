from __future__ import annotations

import sys
from pathlib import Path


INCLUDE_DIR = Path(__file__).resolve().parents[1] / "include"
sys.path.insert(0, str(INCLUDE_DIR))
