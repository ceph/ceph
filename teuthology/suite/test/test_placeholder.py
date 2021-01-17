from teuthology.suite.placeholder import (
    substitute_placeholders, dict_templ, Placeholder
)


class TestPlaceholder(object):
    def test_substitute_placeholders(self):
        suite_hash = 'suite_hash'
        input_dict = dict(
            suite='suite',
            suite_branch='suite_branch',
            suite_hash=suite_hash,
            ceph_branch='ceph_branch',
            ceph_hash='ceph_hash',
            teuthology_branch='teuthology_branch',
            teuthology_sha1='teuthology_sha1',
            machine_type='machine_type',
            distro='distro',
            distro_version='distro_version',
            archive_upload='archive_upload',
            archive_upload_key='archive_upload_key',
            suite_repo='https://example.com/ceph/suite.git',
            suite_relpath='',
            ceph_repo='https://example.com/ceph/ceph.git',
        )
        output_dict = substitute_placeholders(dict_templ, input_dict)
        assert output_dict['suite'] == 'suite'
        assert output_dict['suite_sha1'] == suite_hash
        assert isinstance(dict_templ['suite'], Placeholder)
        assert isinstance(
            dict_templ['overrides']['admin_socket']['branch'],
            Placeholder)

    def test_null_placeholders_dropped(self):
        input_dict = dict(
            suite='suite',
            suite_branch='suite_branch',
            suite_hash='suite_hash',
            ceph_branch='ceph_branch',
            ceph_hash='ceph_hash',
            teuthology_branch='teuthology_branch',
            teuthology_sha1='teuthology_sha1',
            machine_type='machine_type',
            archive_upload='archive_upload',
            archive_upload_key='archive_upload_key',
            distro=None,
            distro_version=None,
            suite_repo='https://example.com/ceph/suite.git',
            suite_relpath='',
            ceph_repo='https://example.com/ceph/ceph.git',
        )
        output_dict = substitute_placeholders(dict_templ, input_dict)
        assert 'os_type' not in output_dict
