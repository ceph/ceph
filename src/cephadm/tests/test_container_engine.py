from unittest import mock

import pytest

from tests.fixtures import with_cephadm_ctx, import_cephadm

from cephadmlib import container_engines


_find_program_loc = 'cephadmlib.container_engine_base.find_program'
_call_throws_loc = 'cephadmlib.container_engines.call_throws'


def test_container_engine():
    from cephadmlib.container_engine_base import ContainerEngine

    with pytest.raises(NotImplementedError):
        ContainerEngine()

    class PhonyContainerEngine(ContainerEngine):
        EXE = "true"

    with mock.patch(_find_program_loc) as find_program:
        find_program.return_value = "/usr/bin/true"
        pce = PhonyContainerEngine()
        assert str(pce) == "true (/usr/bin/true)"


def test_podman():
    with mock.patch(_find_program_loc) as find_program:
        find_program.return_value = "/usr/bin/podman"
        pm = container_engines.Podman()
        find_program.assert_called()
        with pytest.raises(RuntimeError):
            pm.version
        with mock.patch(_call_throws_loc) as call_throws:
            call_throws.return_value = ("4.9.9", None, None)
            with with_cephadm_ctx([]) as ctx:
                pm.get_version(ctx)
        assert pm.version == (4, 9, 9)
        assert str(pm) == "podman (/usr/bin/podman) version 4.9.9"


def test_podman_badversion():
    with mock.patch(_find_program_loc) as find_program:
        find_program.return_value = "/usr/bin/podman"
        pm = container_engines.Podman()
        find_program.assert_called()
        with mock.patch(_call_throws_loc) as call_throws:
            call_throws.return_value = ("4.10.beta2", None, None)
            with with_cephadm_ctx([]) as ctx:
                with pytest.raises(ValueError):
                    pm.get_version(ctx)


def test_docker():
    with mock.patch(_find_program_loc) as find_program:
        find_program.return_value = "/usr/bin/docker"
        docker = container_engines.Docker()
        assert str(docker) == "docker (/usr/bin/docker)"
