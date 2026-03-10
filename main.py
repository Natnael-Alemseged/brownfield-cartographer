#!/usr/bin/env python3
"""
Entry point for Brownfield Cartographer.

  python main.py survey /path/to/local/repo
  python main.py survey https://github.com/dbt-labs/jaffle_shop.git

Uses src.cli for the actual implementation.
"""

import sys
from pathlib import Path

# Add project root so "src" package is importable
_ROOT = Path(__file__).resolve().parent
if _ROOT not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.cli import main

if __name__ == "__main__":
    sys.exit(main())
