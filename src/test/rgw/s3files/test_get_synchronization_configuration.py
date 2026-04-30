"""Conformance tests for GetSynchronizationConfiguration.

Smithy reference: com.amazonaws.s3files#GetSynchronizationConfiguration.
Errors: ValidationException, ResourceNotFoundException,
InternalServerException.
"""

import pytest

from . import errors


_MIN_IMPORT_RULES = [
    {
        "prefix": "",
        "trigger": "ON_FILE_ACCESS",
        "sizeLessThan": 1024 * 1024,
    }
]
_MIN_EXPIRATION_RULES = [{"daysAfterLastAccess": 7}]


@pytest.mark.conformance
def test_get_after_put(s3files_client, test_file_system):
    fs_id = test_file_system['fileSystemId']
    s3files_client.put_synchronization_configuration(
        fileSystemId=fs_id,
        importDataRules=_MIN_IMPORT_RULES,
        expirationDataRules=_MIN_EXPIRATION_RULES,
    )
    got = s3files_client.get_synchronization_configuration(fileSystemId=fs_id)
    assert got['importDataRules'] == _MIN_IMPORT_RULES
    assert got['expirationDataRules'] == _MIN_EXPIRATION_RULES
    assert isinstance(got['latestVersionNumber'], int)


@pytest.mark.conformance
def test_get_on_nonexistent_file_system(s3files_client):
    with pytest.raises(
        s3files_client.exceptions.ResourceNotFoundException
    ) as exc:
        s3files_client.get_synchronization_configuration(
            fileSystemId="fs-no-such-thing-9z9z9z",
        )
    err = exc.value.response.get('Error', {})
    assert err.get('errorCode') == errors.FILE_SYSTEM_NOT_FOUND, err
