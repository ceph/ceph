"""Conformance tests for GetSynchronizationConfiguration.

Smithy reference: com.amazonaws.s3files#GetSynchronizationConfiguration.
Errors: ValidationException, ResourceNotFoundException,
InternalServerException.
"""

import pytest

from . import errors, assert_errorcode, NONEXISTENT_FS_ID


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
            fileSystemId=NONEXISTENT_FS_ID,
        )
    assert_errorcode(exc.value, errors.FILE_SYSTEM_NOT_FOUND)
