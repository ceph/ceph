from unittest.mock import MagicMock, patch

from ceph_node_proxy.redfish_client import RedFishClient


def _logged_in_client() -> RedFishClient:
    client = RedFishClient(host="bmc", username="u", password="p")
    client.token = "tok"
    client.location = "/redfish/v1/SessionService/Sessions/1"
    return client


def test_logout_empty_body() -> None:
    client = _logged_in_client()
    with (
        patch.object(client, "is_logged_in", return_value=True),
        patch.object(client, "query", return_value=(MagicMock(), "", 204)),
    ):
        result = client.logout()
    assert result == {}
    assert client.token == ""
    assert client.location == ""


def test_logout_json_body() -> None:
    client = _logged_in_client()
    with (
        patch.object(client, "is_logged_in", return_value=True),
        patch.object(client, "query", return_value=(MagicMock(), '{"ok": true}', 200)),
    ):
        result = client.logout()
    assert result == {"ok": True}
    assert client.token == ""
    assert client.location == ""


def test_logout_invalid_json_body() -> None:
    client = _logged_in_client()
    with (
        patch.object(client, "is_logged_in", return_value=True),
        patch.object(client, "query", return_value=(MagicMock(), "not-json", 200)),
    ):
        result = client.logout()
    assert result == {}
    assert client.token == ""
    assert client.location == ""
