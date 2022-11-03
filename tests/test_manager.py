import manager
import pytest


@pytest.fixture
def client():
    manager.config = dict(vehicles=[])
    return manager.app.test_client()


def test_ready(client):
    rv = client.get('/ready')
    assert b'Yes' in rv.data


def test_job(client, monkeypatch):
    def set_job(*args):
        return args

    monkeypatch.setattr(manager.fleetManager, "set_job", set_job)

    rv = client.post('/job', json={'fromCity': 'Basel', 'toCity': 'Aarau'})
    assert rv.data == b'["Basel","Aarau"]\n'