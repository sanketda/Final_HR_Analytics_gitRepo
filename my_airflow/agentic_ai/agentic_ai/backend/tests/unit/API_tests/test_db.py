import pytest
from databricks_monitoring.API.app.api.api import get_db

def test_db_generator():
    db_gen = get_db()
    db = next(db_gen)   
    assert db is not None
    db_gen.close()
