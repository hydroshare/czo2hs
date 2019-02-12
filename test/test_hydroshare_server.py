import requests

from settings import HS_URL


def test_hydroshare_server():
    resp = requests.get(HS_URL)
    assert resp.status_code == 200
