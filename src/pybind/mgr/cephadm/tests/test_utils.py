import pytest

from orchestrator import OrchestratorError
from cephadm.utils import name_to_auth_entity

def test_name_to_auth_entity(fs):

    for daemon_type in ['rgw', 'rbd-mirror', 'nfs', "iscsi"]:
        assert "client.%s.id1" % (daemon_type) == name_to_auth_entity(daemon_type, "id1", "host")
        assert "client.%s.id1" % (daemon_type) == name_to_auth_entity(daemon_type, "id1", "")
        assert "client.%s.id1" % (daemon_type) == name_to_auth_entity(daemon_type, "id1")

    assert "client.crash.host" == name_to_auth_entity("crash", "id1", "host")
    with pytest.raises(OrchestratorError):
        t = name_to_auth_entity("crash", "id1", "")
        t = name_to_auth_entity("crash", "id1")

    assert "mon." == name_to_auth_entity("mon", "id1", "host")
    assert "mon." == name_to_auth_entity("mon", "id1", "")
    assert "mon." == name_to_auth_entity("mon", "id1")

    assert "mgr.id1" == name_to_auth_entity("mgr", "id1", "host")
    assert "mgr.id1" == name_to_auth_entity("mgr", "id1", "")
    assert "mgr.id1" == name_to_auth_entity("mgr", "id1")

    for daemon_type in ["osd", "mds", "client"]:
        assert "%s.id1" % daemon_type == name_to_auth_entity(daemon_type, "id1", "host")
        assert "%s.id1" % daemon_type == name_to_auth_entity(daemon_type, "id1", "")
        assert "%s.id1" % daemon_type == name_to_auth_entity(daemon_type, "id1")

    with pytest.raises(OrchestratorError):
         name_to_auth_entity("whatever", "id1", "host")
         name_to_auth_entity("whatever", "id1", "")
         name_to_auth_entity("whatever", "id1")
