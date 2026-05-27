"""Conformance tests for PutSynchronizationConfiguration.

Smithy reference: com.amazonaws.s3files#PutSynchronizationConfiguration.
Errors: ConflictException, ValidationException,
ResourceNotFoundException, InternalServerException.

NOTE: RGW v1 stores the configuration but does not act on it
(see design doc). These tests assert shape round-tripping and
the declared error set; they do not assert any actual data sync
behavior.
"""

import pytest

from . import errors, assert_errorcode, validation_excs, NONEXISTENT_FS_ID


_MIN_IMPORT_RULES = [
    {
        "prefix": "",
        "trigger": "ON_DIRECTORY_FIRST_ACCESS",
        "sizeLessThan": 10 * 1024 * 1024,
    }
]
_MIN_EXPIRATION_RULES = [{"daysAfterLastAccess": 30}]


@pytest.mark.conformance
def test_put_initial_round_trip(s3files_client, test_file_system):
    """Initial put + get returns the same configuration."""
    fs_id = test_file_system['fileSystemId']
    s3files_client.put_synchronization_configuration(
        fileSystemId=fs_id,
        importDataRules=_MIN_IMPORT_RULES,
        expirationDataRules=_MIN_EXPIRATION_RULES,
    )
    got = s3files_client.get_synchronization_configuration(fileSystemId=fs_id)
    assert got['importDataRules'] == _MIN_IMPORT_RULES
    assert got['expirationDataRules'] == _MIN_EXPIRATION_RULES
    assert 'latestVersionNumber' in got


@pytest.mark.conformance
def test_put_optimistic_concurrency_match(s3files_client, test_file_system):
    """Putting again with the current latestVersionNumber succeeds."""
    fs_id = test_file_system['fileSystemId']
    s3files_client.put_synchronization_configuration(
        fileSystemId=fs_id,
        importDataRules=_MIN_IMPORT_RULES,
        expirationDataRules=_MIN_EXPIRATION_RULES,
    )
    got = s3files_client.get_synchronization_configuration(fileSystemId=fs_id)
    s3files_client.put_synchronization_configuration(
        fileSystemId=fs_id,
        latestVersionNumber=got['latestVersionNumber'],
        importDataRules=_MIN_IMPORT_RULES,
        expirationDataRules=[{"daysAfterLastAccess": 60}],
    )


@pytest.mark.conformance
def test_put_optimistic_concurrency_mismatch(s3files_client, test_file_system):
    """Putting with a stale latestVersionNumber → ConflictException."""
    fs_id = test_file_system['fileSystemId']
    s3files_client.put_synchronization_configuration(
        fileSystemId=fs_id,
        importDataRules=_MIN_IMPORT_RULES,
        expirationDataRules=_MIN_EXPIRATION_RULES,
    )
    with pytest.raises(s3files_client.exceptions.ConflictException):
        s3files_client.put_synchronization_configuration(
            fileSystemId=fs_id,
            latestVersionNumber=999999,
            importDataRules=_MIN_IMPORT_RULES,
            expirationDataRules=_MIN_EXPIRATION_RULES,
        )


@pytest.mark.conformance
def test_put_missing_import_rules(s3files_client, test_file_system):
    with pytest.raises(validation_excs(s3files_client)):
        s3files_client.put_synchronization_configuration(
            fileSystemId=test_file_system['fileSystemId'],
            expirationDataRules=_MIN_EXPIRATION_RULES,
        )


@pytest.mark.conformance
def test_put_missing_expiration_rules(s3files_client, test_file_system):
    with pytest.raises(validation_excs(s3files_client)):
        s3files_client.put_synchronization_configuration(
            fileSystemId=test_file_system['fileSystemId'],
            importDataRules=_MIN_IMPORT_RULES,
        )


@pytest.mark.conformance
def test_put_empty_import_rules_rejected(s3files_client, test_file_system):
    """Smithy length 1..10 — empty list is invalid (boto3 rejects
    client-side via the @length trait)."""
    with pytest.raises(validation_excs(s3files_client)):
        s3files_client.put_synchronization_configuration(
            fileSystemId=test_file_system['fileSystemId'],
            importDataRules=[],
            expirationDataRules=_MIN_EXPIRATION_RULES,
        )


@pytest.mark.conformance
def test_put_too_many_expiration_rules_rejected(
    s3files_client, test_file_system
):
    """Smithy length 1..1 — exactly one expiration rule allowed."""
    with pytest.raises(s3files_client.exceptions.ValidationException) as exc:
        s3files_client.put_synchronization_configuration(
            fileSystemId=test_file_system['fileSystemId'],
            importDataRules=_MIN_IMPORT_RULES,
            expirationDataRules=[
                {"daysAfterLastAccess": 30},
                {"daysAfterLastAccess": 60},
            ],
        )
    assert_errorcode(exc.value, errors.INVALID_SYNC_RULES)


@pytest.mark.conformance
def test_put_invalid_trigger_rejected(s3files_client, test_file_system):
    """ImportTrigger is an enum: ON_DIRECTORY_FIRST_ACCESS or ON_FILE_ACCESS."""
    with pytest.raises(s3files_client.exceptions.ValidationException) as exc:
        s3files_client.put_synchronization_configuration(
            fileSystemId=test_file_system['fileSystemId'],
            importDataRules=[{
                "prefix": "",
                "trigger": "INVALID_TRIGGER_VALUE",
                "sizeLessThan": 1024,
            }],
            expirationDataRules=_MIN_EXPIRATION_RULES,
        )
    assert_errorcode(exc.value, errors.INVALID_SYNC_RULES)


@pytest.mark.conformance
def test_put_days_out_of_range(s3files_client, test_file_system):
    """daysAfterLastAccess range is 1..365."""
    with pytest.raises(s3files_client.exceptions.ValidationException) as exc:
        s3files_client.put_synchronization_configuration(
            fileSystemId=test_file_system['fileSystemId'],
            importDataRules=_MIN_IMPORT_RULES,
            expirationDataRules=[{"daysAfterLastAccess": 1000}],
        )
    assert_errorcode(exc.value, errors.INVALID_SYNC_RULES)


@pytest.mark.conformance
def test_put_on_nonexistent_file_system(s3files_client):
    with pytest.raises(
        s3files_client.exceptions.ResourceNotFoundException
    ) as exc:
        s3files_client.put_synchronization_configuration(
            fileSystemId=NONEXISTENT_FS_ID,
            importDataRules=_MIN_IMPORT_RULES,
            expirationDataRules=_MIN_EXPIRATION_RULES,
        )
    assert_errorcode(exc.value, errors.FILE_SYSTEM_NOT_FOUND)
