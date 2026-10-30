"""
Tests unitaires pour la logique d'ingestion. Aucune dependance a Spark,
Airflow ou a un vrai reseau/MinIO : tout est simule (mocke). Pour les
lancer :

    pip install pytest requests
    python -m pytest tests/ -v
"""

import importlib.util
import pathlib
import time
import pytest
import requests

# On charge le module directement par son chemin de fichier plutot que par
# un import classique, c'est plus fiable sous Windows et dans les dossiers
# synchronises par OneDrive, ou sys.path.insert pose parfois probleme.
_INGESTION_FILE = pathlib.Path(__file__).resolve().parent.parent / "ingestion" / "fetch_taxi_data.py"
_spec = importlib.util.spec_from_file_location("fetch_taxi_data", _INGESTION_FILE)
ingestion = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ingestion)


class FakeResponse:
    """
    Reponse HTTP factice, utilisee a la place d'une vraie reponse requests
    pour simuler l'API SODA sans faire de vrai appel reseau pendant les tests.
    """

    def __init__(self, json_data, status_code=200):
        self._json_data = json_data
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"status {self.status_code}")

    def json(self):
        return self._json_data


def test_fetch_page_builds_correct_params(monkeypatch):
    """
    Verifie que la requete envoyee a l'API contient bien le filtre attendu
    sur trip_start_timestamp, ainsi que la bonne limite de page et le bon
    tri.
    """
    captured = {}

    def fake_get(url, params=None, timeout=None):
        captured["url"] = url
        captured["params"] = params
        captured["timeout"] = timeout
        return FakeResponse([{"trip_id": "abc", "trip_start_timestamp": "2023-01-02T00:00:00"}])

    monkeypatch.setattr(requests, "get", fake_get)

    rows = ingestion.fetch_page("2023-01-01T00:00:00")

    assert captured["url"] == ingestion.BASE_URL
    assert "trip_start_timestamp >=" in captured["params"]["$where"]
    assert captured["params"]["$limit"] == ingestion.PAGE_SIZE
    assert captured["params"]["$order"] == "trip_start_timestamp"
    assert len(rows) == 1


def test_fetch_page_retries_on_timeout_then_succeeds(monkeypatch):
    """
    Verifie que fetch_page reessaie automatiquement apres un timeout
    ponctuel, au lieu d'echouer directement. Ce comportement reproduit un
    incident reel rencontre avec l'API de Chicago pendant le developpement.
    """
    call_count = {"n": 0}

    def flaky_get(url, params=None, timeout=None):
        call_count["n"] += 1
        if call_count["n"] < 3:
            raise requests.exceptions.ReadTimeout("simulated timeout")
        return FakeResponse([{"trip_id": "xyz", "trip_start_timestamp": "2023-01-02T00:00:00"}])

    monkeypatch.setattr(requests, "get", flaky_get)
    monkeypatch.setattr(time, "sleep", lambda _: None)  # pour ne pas vraiment attendre pendant le test

    rows = ingestion.fetch_page("2023-01-01T00:00:00")

    assert call_count["n"] == 3
    assert len(rows) == 1


def test_fetch_page_raises_after_max_retries(monkeypatch):
    """
    Verifie que fetch_page finit par abandonner et lever l'exception,
    une fois toutes les tentatives epuisees, plutot que de boucler
    indefiniment.
    """

    def always_timeout(url, params=None, timeout=None):
        raise requests.exceptions.ReadTimeout("simulated persistent timeout")

    monkeypatch.setattr(requests, "get", always_timeout)
    monkeypatch.setattr(time, "sleep", lambda _: None)

    with pytest.raises(requests.exceptions.ReadTimeout):
        ingestion.fetch_page("2023-01-01T00:00:00")


def test_ensure_bucket_creates_when_missing():
    """
    Verifie que ensure_bucket ne cree le bucket que s'il n'existe pas
    deja, pour que le script reste sans effet de bord si on le relance
    plusieurs fois.
    """

    class FakeS3:
        def __init__(self, existing_buckets):
            self.existing = existing_buckets
            self.created = []

        def list_buckets(self):
            return {"Buckets": [{"Name": name} for name in self.existing]}

        def create_bucket(self, Bucket):
            self.created.append(Bucket)

    s3_missing = FakeS3(existing_buckets=[])
    ingestion.ensure_bucket(s3_missing)
    assert s3_missing.created == [ingestion.BUCKET]

    s3_present = FakeS3(existing_buckets=[ingestion.BUCKET])
    ingestion.ensure_bucket(s3_present)
    assert s3_present.created == []