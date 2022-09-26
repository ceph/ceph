from unittest import mock

import pytest

from tests.fixtures import with_cephadm_ctx

with mock.patch("builtins.open", create=True):
    from importlib.machinery import SourceFileLoader

    cephadm = SourceFileLoader("cephadm", "cephadm.py").load_module()


def test_container_engine():
    with pytest.raises(NotImplementedError):
        cephadm.ContainerEngine()

    class PhonyContainerEngine(cephadm.ContainerEngine):
        EXE = "true"

    with mock.patch("cephadm.find_program") as find_program:
        find_program.return_value = "/usr/bin/true"
        pce = PhonyContainerEngine()
        assert str(pce) == "true (/usr/bin/true)"


def test_podman():
    with mock.patch("cephadm.find_program") as find_program:
        find_program.return_value = "/usr/bin/podman"
        pm = cephadm.Podman()
        find_program.assert_called()
        with pytest.raises(RuntimeError):
            pm.version
        with mock.patch("cephadm.call_throws") as call_throws:
            call_throws.return_value = ("4.9.9", None, None)
            with with_cephadm_ctx([]) as ctx:
                pm.get_version(ctx)
        assert pm.version == (4, 9, 9)
        assert str(pm) == "podman (/usr/bin/podman) version 4.9.9"


def test_podman_badversion():
    with mock.patch("cephadm.find_program") as find_program:
        find_program.return_value = "/usr/bin/podman"
        pm = cephadm.Podman()
        find_program.assert_called()
        with mock.patch("cephadm.call_throws") as call_throws:
            call_throws.return_value = ("4.10.beta2", None, None)
            with with_cephadm_ctx([]) as ctx:
                with pytest.raises(ValueError):
                    pm.get_version(ctx)


def test_docker():
    with mock.patch("cephadm.find_program") as find_program:
        find_program.return_value = "/usr/bin/docker"
        docker = cephadm.Docker()
        assert str(docker) == "docker (/usr/bin/docker)"
