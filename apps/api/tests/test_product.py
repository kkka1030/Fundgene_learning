from fastapi.testclient import TestClient


def test_product_endpoint(client: TestClient) -> None:
    response = client.get("/api/product")

    assert response.status_code == 200
    payload = response.json()
    assert payload["name"] == "FundGene"
    assert payload["phase"] == "phase-4-mvp-spine-c"
    assert any(module["slug"] == "onboarding" for module in payload["modules"])
    assert any(module["slug"] == "coach" for module in payload["modules"])
    assert any(module["slug"] == "simulation" for module in payload["modules"])
