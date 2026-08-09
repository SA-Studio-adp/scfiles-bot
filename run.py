#!/usr/bin/env python3
"""
run.py — repo-root entrypoint.

Usage:
    python run.py

This just makes sure scfiles_bot/ is importable (its modules use flat
imports like `from config import ...`, so the package directory itself
needs to be on sys.path — this script handles that for you) and then
calls scfiles_bot/main.py's main().
"""
import asyncio
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "scfiles_bot"))

from main import main  # noqa: E402

if __name__ == "__main__":
    asyncio.run(main())
