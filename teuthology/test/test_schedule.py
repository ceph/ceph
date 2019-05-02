from teuthology.schedule import build_config
from teuthology.misc import get_user


class TestSchedule(object):
    basic_args = {
        '--verbose': False,
        '--owner': 'OWNER',
        '--description': 'DESC',
        '--email': 'EMAIL',
        '--first-in-suite': False,
        '--last-in-suite': True,
        '--name': 'NAME',
        '--worker': 'tala',
        '--timeout': '6',
        '--priority': '99',
        # TODO: make this work regardless of $PWD
        #'<conf_file>': ['../../examples/3node_ceph.yaml',
        #                '../../examples/3node_rgw.yaml'],
        }

    def test_basic(self):
        expected = {
            'description': 'DESC',
            'email': 'EMAIL',
            'first_in_suite': False,
            'last_in_suite': True,
            'machine_type': 'tala',
            'name': 'NAME',
            'owner': 'OWNER',
            'priority': 99,
            'results_timeout': '6',
            'verbose': False,
            'tube': 'tala',
        }

        job_dict = build_config(self.basic_args)
        assert job_dict == expected

    def test_owner(self):
        args = self.basic_args
        args['--owner'] = None
        job_dict = build_config(self.basic_args)
        assert job_dict['owner'] == 'scheduled_%s' % get_user()

