import pytest

from ceph.deployment.service_spec import RGWGapListSpec, ServiceSpec, SpecValidationError


class TestRGWGapListSpec:
    def test_defaults(self):
        spec = RGWGapListSpec()
        spec.validate()
        assert spec.service_type == 'rgw-gap-list'
        assert spec.scan_interval is None
        assert spec.scan_timeout is None
        assert spec.max_age is None
        assert spec.pool is None
        assert spec.sync_pool is None
        assert spec.inflight is None

    def test_custom_values(self):
        spec = RGWGapListSpec(
            scan_interval=86400,
            scan_timeout=3600,
            max_age=1800,
            pool='mypool.data',
            sync_pool='mypool.index',
            namespace='my-namespace',
            bucket_list='bucket1 bucket2',
            exclude_list='skipme',
            inflight=5000,
            extra_opts='-vv',
        )
        spec.validate()
        assert spec.scan_interval == 86400
        assert spec.scan_timeout == 3600
        assert spec.max_age == 1800
        assert spec.pool == 'mypool.data'
        assert spec.sync_pool == 'mypool.index'
        assert spec.namespace == 'my-namespace'
        assert spec.bucket_list == 'bucket1 bucket2'
        assert spec.inflight == 5000
        assert spec.extra_opts == '-vv'

    def test_scan_interval_too_small(self):
        spec = RGWGapListSpec(scan_interval=100)
        with pytest.raises(SpecValidationError, match='scan_interval must be at least 300'):
            spec.validate()

    def test_scan_timeout_too_small(self):
        spec = RGWGapListSpec(scan_timeout=10)
        with pytest.raises(SpecValidationError, match='scan_timeout must be at least 60'):
            spec.validate()

    def test_max_age_negative(self):
        spec = RGWGapListSpec(max_age=-1)
        with pytest.raises(SpecValidationError, match='max_age must be non-negative'):
            spec.validate()

    def test_inflight_zero(self):
        spec = RGWGapListSpec(inflight=0)
        with pytest.raises(SpecValidationError, match='inflight must be at least 1'):
            spec.validate()

    def test_timeout_exceeds_interval(self):
        spec = RGWGapListSpec(scan_interval=3600, scan_timeout=7200)
        with pytest.raises(
                SpecValidationError,
                match='scan_timeout must be less than scan_interval'):
            spec.validate()

    def test_timeout_equals_interval(self):
        spec = RGWGapListSpec(scan_interval=3600, scan_timeout=3600)
        with pytest.raises(
                SpecValidationError,
                match='scan_timeout must be less than scan_interval'):
            spec.validate()

    def test_timeout_less_than_interval(self):
        spec = RGWGapListSpec(scan_interval=3600, scan_timeout=1800)
        spec.validate()  # should not raise

    def test_small_interval_with_default_timeout_rejected(self):
        # Only scan_interval is set; the effective (default) scan_timeout
        # is larger, so validation must catch the invariant violation.
        spec = RGWGapListSpec(scan_interval=600)
        with pytest.raises(
                SpecValidationError,
                match='scan_timeout must be less than scan_interval'):
            spec.validate()

    def test_defaults_constants(self):
        assert RGWGapListSpec.DEFAULT_SCAN_INTERVAL == 1296000
        assert RGWGapListSpec.DEFAULT_SCAN_TIMEOUT == 86400
        assert RGWGapListSpec.DEFAULT_MAX_AGE == 604800
        assert RGWGapListSpec.DEFAULT_INFLIGHT == 15000

    def test_from_json(self):
        spec_dict = {
            'service_type': 'rgw-gap-list',
            'placement': {'count': 2},
            'spec': {
                'scan_interval': 86400,
                'scan_timeout': 3600,
                'max_age': 3600,
                'pool': 'test.pool',
                'sync_pool': 'test.index',
            }
        }
        spec = ServiceSpec.from_json(spec_dict)
        assert isinstance(spec, RGWGapListSpec)
        spec.validate()
        assert spec.scan_interval == 86400
        assert spec.pool == 'test.pool'

    def test_max_age_zero_roundtrip(self):
        spec = RGWGapListSpec(scan_interval=86400, scan_timeout=3600, max_age=0)
        spec.validate()
        restored = ServiceSpec.from_json(spec.to_json())
        assert isinstance(restored, RGWGapListSpec)
        assert restored.max_age == 0

    def test_to_json_roundtrip(self):
        spec = RGWGapListSpec(
            scan_interval=86400,
            scan_timeout=3600,
            pool='mypool',
        )
        j = spec.to_json()
        restored = ServiceSpec.from_json(j)
        assert isinstance(restored, RGWGapListSpec)
        assert restored.scan_interval == 86400
        assert restored.pool == 'mypool'

    def test_extra_opts_shell_injection(self):
        spec = RGWGapListSpec(extra_opts='-vv --debug')
        spec.validate()  # valid

        spec = RGWGapListSpec(extra_opts='; rm -rf /')
        with pytest.raises(SpecValidationError, match='disallowed characters'):
            spec.validate()

        spec = RGWGapListSpec(extra_opts='$(cmd)')
        with pytest.raises(SpecValidationError, match='disallowed characters'):
            spec.validate()

    def test_extra_opts_unterminated_quote(self):
        # An unmatched quote would otherwise produce a syntactically broken
        # run script and silently fail every scan.
        spec = RGWGapListSpec(extra_opts="-vv 'foo")
        with pytest.raises(SpecValidationError, match='invalid quoting'):
            spec.validate()

    def test_non_string_fields_rejected(self):
        # e.g. YAML `pool: [pool1, pool2]` -> a list; must be rejected at
        # apply time rather than failing later at data_pool.split().
        for field in ('pool', 'sync_pool', 'namespace', 'bucket_list',
                      'exclude_list', 'extra_opts'):
            spec = RGWGapListSpec(**{field: ['a', 'b']})
            with pytest.raises(SpecValidationError, match=f'{field} must be a string'):
                spec.validate()

    def test_extra_opts_hash_is_valid(self):
        # '#' is not a shell-injection risk and is quoted at deploy time,
        # so it must pass validation rather than be rejected.
        spec = RGWGapListSpec(extra_opts='-vv #')
        spec.validate()  # should not raise

    def test_cls_mapping(self):
        cls = ServiceSpec._cls('rgw-gap-list')
        assert cls == RGWGapListSpec


def test_get_auth_entity():
    from cephadm.services.cephadmservice import get_auth_entity
    assert get_auth_entity('rgw-gap-list', 'host.abc') == 'client.rgw-gap-list.host.abc'


def test_name_to_config_section():
    from cephadm.utils import name_to_config_section
    assert str(name_to_config_section('rgw-gap-list.host.abc')) == 'client.rgw-gap-list.host.abc'


def test_allow_colo():
    from cephadm.services.rgw_gap_list import RgwGapListService
    assert RgwGapListService.allow_colo(None) is True
