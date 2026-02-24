# tests/conftest.py
import sys
from pathlib import Path

# repo root: <repo>/tests/conftest.py -> <repo>
REPO_ROOT = Path(__file__).resolve().parents[1]

# Always add the main src/ so absolute package imports work:
#   from databricks_monitoring.API.app.main import app
#   from data_quality.loader import load_data
paths = [
    REPO_ROOT / "src",  # enables 'databricks_monitoring', 'data_quality'

    # The code in src uses bare imports like 'from app ...' and 'from send_mail ...'.
    # Make those folders act like top-level modules without changing src code.
    REPO_ROOT / "src" / "databricks_monitoring" / "API",                 # enables 'import app' (app/… lives here)
    REPO_ROOT / "src" / "databricks_monitoring" / "API" / "app",         # safety for subfolder bare imports (rare)
    REPO_ROOT / "src" / "databricks_monitoring" / "webhook_diagnosis",   # enables 'import send_mail', 'import tasks', 'import celery_worker'
    REPO_ROOT / "src" / "data_quality",                                  # if any bare imports exist there
]

# Prepend (highest priority) and only if they exist / not already present
for p in paths:
    if p.exists():
        p_str = str(p)
        if p_str not in sys.path:
            sys.path.insert(0, p_str)
