import datetime

import pytest

from ceph.utils import datetime_now, datetime_to_str, str_to_datetime


def test_datetime_to_str_1():
    dt = datetime.datetime.now()
    assert type(datetime_to_str(dt)) is str


def test_datetime_to_str_2():
    # note: tz isn't specified in the string, so explicitly store this as UTC
    dt = datetime.datetime.strptime(
        '2019-04-24T17:06:53.039991',
        '%Y-%m-%dT%H:%M:%S.%f'
    ).replace(tzinfo=datetime.timezone.utc)
    assert datetime_to_str(dt) == '2019-04-24T17:06:53.039991Z'


def test_datetime_to_str_3():
    dt = datetime.datetime.strptime('2020-11-02T04:40:12.748172-0800',
                                    '%Y-%m-%dT%H:%M:%S.%f%z')
    assert datetime_to_str(dt) == '2020-11-02T12:40:12.748172Z'


def test_str_to_datetime_1():
    dt = str_to_datetime('2020-03-03T09:21:43.636153304Z')
    assert type(dt) is datetime.datetime
    assert dt.tzinfo is not None


def test_str_to_datetime_2():
    dt = str_to_datetime('2020-03-03T15:52:30.136257504-0600')
    assert type(dt) is datetime.datetime
    assert dt.tzinfo is not None


def test_str_to_datetime_3():
    dt = str_to_datetime('2020-03-03T15:52:30.136257504')
    assert type(dt) is datetime.datetime
    assert dt.tzinfo is not None


def test_str_to_datetime_invalid_format_1():
    with pytest.raises(ValueError):
        str_to_datetime('2020-03-03 15:52:30.136257504')


def test_str_to_datetime_invalid_format_2():
    with pytest.raises(ValueError):
        str_to_datetime('2020-03-03')


def test_datetime_now_1():
    dt = str_to_datetime('2020-03-03T09:21:43.636153304Z')
    dt_now = datetime_now()
    assert type(dt_now) is datetime.datetime
    assert dt_now.tzinfo is not None
    assert dt < dt_now


def _strptime_reference(string):
    # How str_to_datetime() parsed the datetime_to_str() format before the
    # fast path: 'Z' replaced by '-0000', then strptime() with '%z'.
    return datetime.datetime.strptime(string[:-1] + '-0000',
                                      '%Y-%m-%dT%H:%M:%S.%f%z')


@pytest.mark.parametrize('string', [
    '2020-03-03T09:21:43.636153Z',
    '2024-02-29T23:59:59.999999Z',
    '1970-01-01T00:00:00.000000Z',
    '0001-01-01T00:00:00.000000Z',
    '9999-12-31T23:59:59.999999Z',
])
def test_str_to_datetime_datetime_to_str_format(string):
    dt = str_to_datetime(string)
    ref = _strptime_reference(string)
    assert dt == ref
    assert dt.utcoffset() == ref.utcoffset()
    assert repr(dt) == repr(ref)
    assert dt.tzinfo is datetime.timezone.utc


@pytest.mark.parametrize('string', [
    '2023-02-29T00:00:00.000000Z',
    '2020-04-31T00:00:00.000000Z',
    '2020-13-01T00:00:00.000000Z',
    '2020-00-01T00:00:00.000000Z',
    '2020-01-00T00:00:00.000000Z',
    '2020-01-01T24:00:00.000000Z',
    '2020-01-01T23:60:00.000000Z',
    '2020-01-01T23:59:60.000000Z',
    '0000-01-01T00:00:00.000000Z',
])
def test_str_to_datetime_datetime_to_str_format_out_of_range(string):
    # Same error as before the fast path.
    with pytest.raises(ValueError, match='does not match one of the formats'):
        str_to_datetime(string)


@pytest.mark.parametrize('dt', [
    datetime.datetime(2020, 3, 3, 9, 21, 43, 636153,
                      tzinfo=datetime.timezone.utc),
    datetime.datetime(2020, 3, 3, 15, 52, 30, 1,
                      tzinfo=datetime.timezone(datetime.timedelta(hours=-6))),
    datetime.datetime(2020, 3, 3, 15, 52, 30, tzinfo=datetime.timezone.utc),
])
def test_str_to_datetime_roundtrip(dt):
    assert str_to_datetime(datetime_to_str(dt)) == dt
