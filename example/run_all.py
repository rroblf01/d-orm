"""
Single entry point: runs setup + sync demo + async demo.

    uv run python example/run_all.py
"""
import os
import sys
import runpy

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

BASE = os.path.dirname(os.path.abspath(__file__))

for script in ["setup_db.py", "demo_sync.py", "demo_async.py"]:
    runpy.run_path(os.path.join(BASE, script), run_name="__main__")
