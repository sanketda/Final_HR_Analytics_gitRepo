from fastapi.testclient import TestClient
from databricks_monitoring.API.app.main import app

client = TestClient(app)

def test_existing_routes():
    # Collect all registered paths
    routes = [route.path for route in app.routes]

    # Example endpoint test
    if "/v1/example" in routes:
        response = client.get("/v1/example")
        assert response.status_code == 200
