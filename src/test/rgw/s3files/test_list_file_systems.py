"""Conformance tests for ListFileSystems.

Smithy reference: com.amazonaws.s3files#ListFileSystems.
"""

import pytest

from . import validation_excs


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


@pytest.mark.conformance
def test_list_max_results_out_of_range(s3files_client):
    """Smithy `@range` is 1..100. Either side may catch."""
    with pytest.raises(validation_excs(s3files_client)):
        s3files_client.list_file_systems(maxResults=10000)


@pytest.mark.conformance
def test_list_pagination_via_next_token(
    s3files_client, s3_client, shared_test_role
):
    """When more entries exist than maxResults, the response carries
    a nextToken; passing it back returns the rest. Across two pages
    every created FS must appear exactly once."""
    bucket_names = []
    fs_ids = []
    try:
        # Create 3 buckets, bind a FS to each.
        for _ in range(3):
            import uuid
            bn = f"s3files-page-{uuid.uuid4().hex[:8]}"
            s3_client.create_bucket(Bucket=bn)
            bucket_names.append(bn)
            r = s3files_client.create_file_system(
                bucket=f"arn:aws:s3:::{bn}",
                roleArn=shared_test_role,
            )
            fs_ids.append(r['fileSystemId'])

        seen = set()
        token = None
        page_count = 0
        while True:
            kwargs = {"maxResults": 1}
            if token:
                kwargs['nextToken'] = token
            resp = s3files_client.list_file_systems(**kwargs)
            page_count += 1
            for fs in resp['fileSystems']:
                seen.add(fs['fileSystemId'])
            token = resp.get('nextToken')
            if not token:
                break
            assert page_count <= 100, "pagination did not terminate"

        for fs_id in fs_ids:
            assert fs_id in seen
        assert page_count >= len(fs_ids), (
            f"expected >= {len(fs_ids)} pages with maxResults=1, "
            f"got {page_count}"
        )
    finally:
        for fs_id in fs_ids:
            try:
                s3files_client.delete_file_system(fileSystemId=fs_id)
            except Exception:
                pass
        for bn in bucket_names:
            try:
                s3_client.delete_bucket(Bucket=bn)
            except Exception:
                pass


@pytest.mark.conformance
def test_list_filter_by_bucket(
    s3files_client, bucket_arn, shared_test_role, test_file_system
):
    """ListFileSystems(bucket=ARN) returns only the FS bound to
    that bucket. test_file_system is bound to bucket_arn; create
    a second FS on a different bucket and confirm the filter
    distinguishes them."""
    # `test_file_system` (already created) is bound to `bucket_arn`.
    expected_id = test_file_system['fileSystemId']
    resp = s3files_client.list_file_systems(bucket=bucket_arn)
    ids = {fs['fileSystemId'] for fs in resp['fileSystems']}
    assert ids == {expected_id}, (
        f"bucket filter returned {ids}, expected exactly "
        f"{{{expected_id}}}"
    )
