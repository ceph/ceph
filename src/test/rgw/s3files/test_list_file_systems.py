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
def test_list_entries_match_get(s3files_client, test_file_system):
    """Each list entry has the same Smithy-modeled fields as the
    corresponding Get response, with matching values. The
    `FileSystemDescription` shape is shared between Get and List."""
    fs_id = test_file_system['fileSystemId']
    got = s3files_client.get_file_system(fileSystemId=fs_id)
    listed = next(
        (fs for fs in s3files_client.list_file_systems()['fileSystems']
         if fs['fileSystemId'] == fs_id),
        None,
    )
    assert listed is not None
    for field in ('fileSystemArn', 'bucket', 'roleArn',
                  'status', 'ownerId', 'creationTime'):
        assert listed.get(field) == got.get(field), field
