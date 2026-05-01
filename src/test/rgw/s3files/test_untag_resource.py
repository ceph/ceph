"""Conformance tests for UntagResource.

Smithy reference: com.amazonaws.s3files#UntagResource.
Errors: InternalServerException, ResourceNotFoundException,
ValidationException.
"""

import pytest

from . import errors, NONEXISTENT_FS_ID, NONEXISTENT_AP_ID


@pytest.fixture(params=["test_file_system", "test_access_point"])
def taggable_arn(request):
    resource = request.getfixturevalue(request.param)
    return resource.get('fileSystemArn') or resource['accessPointArn']


def _tag_keys(s3files_client, arn):
    return {
        t['key']
        for t in s3files_client.list_tags_for_resource(resourceId=arn)['tags']
    }


@pytest.mark.conformance
def test_untag_removes_keys(s3files_client, taggable_arn):
    s3files_client.tag_resource(
        resourceId=taggable_arn,
        tags=[
            {"key": "a", "value": "1"},
            {"key": "b", "value": "2"},
        ],
    )
    s3files_client.untag_resource(
        resourceId=taggable_arn,
        tagKeys=["a"],
    )
    keys = _tag_keys(s3files_client, taggable_arn)
    assert "a" not in keys
    assert "b" in keys


@pytest.mark.conformance
def test_untag_idempotent_on_unknown_key(s3files_client, taggable_arn):
    """Removing a tag key that isn't present is a no-op (no error)."""
    s3files_client.untag_resource(
        resourceId=taggable_arn,
        tagKeys=["never-set"],
    )


@pytest.mark.conformance
def test_untag_missing_tag_keys(s3files_client, taggable_arn):
    with pytest.raises(s3files_client.exceptions.ValidationException):
        s3files_client.untag_resource(resourceId=taggable_arn)


@pytest.mark.conformance
@pytest.mark.parametrize("bogus_id", [NONEXISTENT_FS_ID, NONEXISTENT_AP_ID])
def test_untag_on_nonexistent(s3files_client, bogus_id):
    with pytest.raises(
        s3files_client.exceptions.ResourceNotFoundException
    ) as exc:
        s3files_client.untag_resource(
            resourceId=bogus_id,
            tagKeys=["any"],
        )
    err = exc.value.response.get('Error', {})
    assert err.get('errorCode') in (
        errors.FILE_SYSTEM_NOT_FOUND,
        errors.ACCESS_POINT_NOT_FOUND,
    ), err
