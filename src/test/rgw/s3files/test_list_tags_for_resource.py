"""Conformance tests for ListTagsForResource.

Smithy reference: com.amazonaws.s3files#ListTagsForResource.
Errors: InternalServerException, ResourceNotFoundException,
ValidationException.

Tagging applies to FileSystem and AccessPoint resources only —
MountTargets don't match the ResourceId pattern. Each test
parametrizes over both resource types.
"""

import pytest

from . import errors, NONEXISTENT_FS_ID, NONEXISTENT_AP_ID


@pytest.fixture(params=["test_file_system", "test_access_point"])
def taggable_arn(request):
    resource = request.getfixturevalue(request.param)
    return resource.get('fileSystemArn') or resource['accessPointArn']


@pytest.mark.conformance
def test_list_tags_returns_list_field(s3files_client, taggable_arn):
    resp = s3files_client.list_tags_for_resource(resourceId=taggable_arn)
    assert 'tags' in resp
    assert isinstance(resp['tags'], list)


@pytest.mark.conformance
def test_list_tags_after_tag_resource(s3files_client, taggable_arn):
    s3files_client.tag_resource(
        resourceId=taggable_arn,
        tags=[{"key": "team", "value": "infra"}],
    )
    resp = s3files_client.list_tags_for_resource(resourceId=taggable_arn)
    keys = {t['key']: t['value'] for t in resp['tags']}
    assert keys.get('team') == 'infra'


@pytest.mark.conformance
@pytest.mark.parametrize("bogus_id", [NONEXISTENT_FS_ID, NONEXISTENT_AP_ID])
def test_list_tags_on_nonexistent(s3files_client, bogus_id):
    with pytest.raises(
        s3files_client.exceptions.ResourceNotFoundException
    ) as exc:
        s3files_client.list_tags_for_resource(resourceId=bogus_id)
    err = exc.value.response.get('Error', {})
    assert err.get('errorCode') in (
        errors.FILE_SYSTEM_NOT_FOUND,
        errors.ACCESS_POINT_NOT_FOUND,
    ), err


@pytest.mark.conformance
def test_list_tags_max_results_out_of_range(s3files_client, taggable_arn):
    """Smithy range is 1..50."""
    with pytest.raises(s3files_client.exceptions.ValidationException):
        s3files_client.list_tags_for_resource(
            resourceId=taggable_arn,
            MaxResults=10000,
        )
