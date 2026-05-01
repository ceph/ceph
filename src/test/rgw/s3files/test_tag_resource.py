"""Conformance tests for TagResource.

Smithy reference: com.amazonaws.s3files#TagResource.
Errors: InternalServerException, ResourceNotFoundException,
ValidationException.
"""

import pytest

from . import errors, assert_errorcode, validation_excs, NONEXISTENT_FS_ID, NONEXISTENT_AP_ID


@pytest.fixture(params=["test_file_system", "test_access_point"])
def taggable_arn(request):
    resource = request.getfixturevalue(request.param)
    # Use bare ids rather than ARNs: AWS Smithy ResourceId
    # accepts both, and ARNs in the URL contain `/` characters
    # (encoded as %2F by boto3) that RGW's canonical-URI
    # handler decodes back to `/`, breaking the sigv4
    # signature match.
    return resource.get('fileSystemId') or resource['accessPointId']


def _tag_map(tags):
    return {t['key']: t['value'] for t in tags}


@pytest.mark.conformance
def test_tag_adds_tags(s3files_client, taggable_arn):
    s3files_client.tag_resource(
        resourceId=taggable_arn,
        tags=[
            {"key": "env", "value": "ci"},
            {"key": "owner", "value": "test"},
        ],
    )
    got = _tag_map(
        s3files_client.list_tags_for_resource(resourceId=taggable_arn)['tags']
    )
    assert got.get('env') == 'ci'
    assert got.get('owner') == 'test'


@pytest.mark.conformance
def test_tag_replaces_existing_value(s3files_client, taggable_arn):
    """A second TagResource with the same key replaces its value."""
    s3files_client.tag_resource(
        resourceId=taggable_arn,
        tags=[{"key": "env", "value": "old"}],
    )
    s3files_client.tag_resource(
        resourceId=taggable_arn,
        tags=[{"key": "env", "value": "new"}],
    )
    got = _tag_map(
        s3files_client.list_tags_for_resource(resourceId=taggable_arn)['tags']
    )
    assert got.get('env') == 'new'


@pytest.mark.conformance
def test_tag_missing_tags(s3files_client, taggable_arn):
    with pytest.raises(validation_excs(s3files_client)):
        s3files_client.tag_resource(resourceId=taggable_arn)


@pytest.mark.conformance
@pytest.mark.parametrize("bogus_id", [NONEXISTENT_FS_ID, NONEXISTENT_AP_ID])
def test_tag_on_nonexistent(s3files_client, bogus_id):
    with pytest.raises(
        s3files_client.exceptions.ResourceNotFoundException
    ) as exc:
        s3files_client.tag_resource(
            resourceId=bogus_id,
            tags=[{"key": "k", "value": "v"}],
        )
    assert_errorcode(exc.value, (errors.FILE_SYSTEM_NOT_FOUND, errors.ACCESS_POINT_NOT_FOUND))
