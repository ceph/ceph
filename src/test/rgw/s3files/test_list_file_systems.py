"""Conformance tests for ListFileSystems.

Smithy reference: com.amazonaws.s3files#ListFileSystems.
"""

import pytest


@pytest.mark.conformance
def test_list_returns_list_field(s3files_client):
    """Response carries a `fileSystems` list, regardless of contents."""
    resp = s3files_client.list_file_systems()
    assert 'fileSystems' in resp
    assert isinstance(resp['fileSystems'], list)


@pytest.mark.conformance
def test_list_includes_created(s3files_client, test_file_system):
    fs_id = test_file_system['fileSystemId']
    resp = s3files_client.list_file_systems()
    ids = {fs['fileSystemId'] for fs in resp['fileSystems']}
    assert fs_id in ids


@pytest.mark.conformance
def test_list_entries_have_required_fields(
    s3files_client, test_file_system
):
    """Each list entry carries the same fields as GetFileSystem."""
    fs_id = test_file_system['fileSystemId']
    resp = s3files_client.list_file_systems()
    entry = next(
        (fs for fs in resp['fileSystems'] if fs['fileSystemId'] == fs_id),
        None,
    )
    assert entry is not None
    for field in ('fileSystemArn', 'bucket', 'status', 'ownerId'):
        assert field in entry, f"missing {field} in list entry: {entry}"
