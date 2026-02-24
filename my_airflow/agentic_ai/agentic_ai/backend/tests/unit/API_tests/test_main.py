from fastapi.testclient import TestClient
from databricks_monitoring.API.app.main import app

client = TestClient(app)

def test_existing_routes():
    routes = [route.path for route in app.routes]

    # Health check test
    if "/v1/health" in routes:
        response = client.get("/v1/health")
        assert response.status_code == 200
