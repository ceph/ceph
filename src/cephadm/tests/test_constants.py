# Tests for the values in cephadmlib/constants.py
#
import pathlib

import pytest

from cephadmlib import constants

CEPH_RELEASE = pathlib.Path(__file__).resolve().parents[2] / 'ceph_release'


def test_default_image_for_stable_release():
    # A stable release must not default to the ceph-ci build of main
    # (https://tracker.ceph.com/issues/82496).
    if not CEPH_RELEASE.exists():
        pytest.skip(f'{CEPH_RELEASE} not found')
    _, name, release_type = CEPH_RELEASE.read_text().split()
    if release_type != 'stable':
        pytest.skip(f'release type is {release_type}')
    assert not constants.DEFAULT_IMAGE_IS_MAIN
    assert 'ceph-ci' not in constants.DEFAULT_IMAGE
    assert constants.DEFAULT_IMAGE_RELEASE == name
