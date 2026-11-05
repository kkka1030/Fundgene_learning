from fastapi.testclient import TestClient

def test_health_endpoint(client: TestClient) -> None:
    response = client.get("/api/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["service"] == "FundGene API"
    assert payload["environment"] == "development"


def test_ready_endpoint_checks_database(client: TestClient) -> None:
    response = client.get("/api/ready")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ready"
    assert payload["database"] == "ok"
