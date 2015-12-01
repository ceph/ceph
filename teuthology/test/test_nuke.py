import json
import datetime

from mock import patch, Mock, DEFAULT

from teuthology import nuke
from teuthology import misc
from teuthology.config import config


class TestNuke(object):

    def test_stale_openstack_volumes(self):
        ctx = Mock()
        ctx.teuthology_config = config
        ctx.dry_run = False
        now = datetime.datetime.strftime(datetime.datetime.now(),
                                         "%Y-%m-%dT%H:%M:%S.000000")
        id = '4bee3af9-febb-40c1-a17e-ff63edb415c5'
        name = 'target1-0'
        volume_list = json.loads(
            '[{'
            ' "ID": "' + id + '"'
            '}]'
        )
        #
        # A volume created a second ago is left untouched
        #
        volume_show = (
            '['
            ' {"Field": "id", "Value": "' + id + '"},'
            ' {"Field": "created_at", "Value": "' + now + '"},'
            ' {"Field": "display_name", "Value": "' + name + '"}'
            ']'
        )
        def sh(cmd):
            if 'volume show' in cmd:
                return volume_show

        with patch.multiple(
                nuke,
                sh=sh,
                openstack_delete_volume=DEFAULT,
                ) as m:
            nuke.stale_openstack_volumes(ctx, volume_list)
            m['openstack_delete_volume'].assert_not_called()

        #
        # A volume created long ago is destroyed
        #
        ancient = "2000-11-02T15:43:12.000000"
        volume_show = (
            '['
            ' {"Field": "id", "Value": "' + id + '"},'
            ' {"Field": "created_at", "Value": "' + ancient + '"},'
            ' {"Field": "display_name", "Value": "' + name + '"}'
            ']'
        )
        def sh(cmd):
            if 'volume show' in cmd:
                return volume_show

        with patch.multiple(
                nuke,
                sh=sh,
                openstack_delete_volume=DEFAULT,
                ) as m:
            nuke.stale_openstack_volumes(ctx, volume_list)
            m['openstack_delete_volume'].assert_called_with(id)

    def test_stale_openstack_nodes(self):
        ctx = Mock()
        ctx.teuthology_config = config
        ctx.dry_run = False
        name = 'target1'
        now = datetime.datetime.strftime(datetime.datetime.now(),
                                         "%Y-%m-%d %H:%M:%S.%f")
        #
        # A node is not of type openstack is left untouched
        #
        with patch.multiple(
                nuke,
                unlock_one=DEFAULT,
                ) as m:
            nuke.stale_openstack_nodes(ctx, {
            }, {
                name: { 'locked_since': now,
                        'machine_type': 'mira', },
            })
            m['unlock_one'].assert_not_called()
        #
        # A node that was just locked and does not have
        # an instance yet is left untouched
        #
        with patch.multiple(
                nuke,
                unlock_one=DEFAULT,
                ) as m:
            nuke.stale_openstack_nodes(ctx, {
            }, {
                name: { 'locked_since': now,
                        'machine_type': 'openstack', },
            })
            m['unlock_one'].assert_not_called()
        #
        # A node that has been locked for some time and
        # has no instance is unlocked.
        #
        ancient = "2000-11-02 15:43:12.000000"
        me = 'loic@dachary.org'
        with patch.multiple(
                nuke,
                unlock_one=DEFAULT,
                ) as m:
            nuke.stale_openstack_nodes(ctx, {
            }, {
                name: { 'locked_since': ancient,
                        'locked_by': me,
                        'machine_type': 'openstack', },
            })
            m['unlock_one'].assert_called_with(
                ctx, name, me)
        #
        # A node that has been locked for some time and
        # has an instance is left untouched
        #
        with patch.multiple(
                nuke,
                unlock_one=DEFAULT,
                ) as m:
            nuke.stale_openstack_nodes(ctx, {
                name: { 'name': name, },
            }, {
                name: { 'locked_since': ancient,
                        'machine_type': 'openstack', },
            })
            m['unlock_one'].assert_not_called()

    def test_stale_openstack_instances(self):
        ctx = Mock()
        ctx.teuthology_config = config
        ctx.dry_run = False
        name = 'target1'
        #
        # An instance created a second ago is left untouched,
        # even when it is not locked.
        #
        with patch.multiple(
                nuke.OpenStackInstance,
                get_created=lambda _: 1,
                __getitem__=lambda _, key: name,
                destroy=DEFAULT,
                ) as m:
            nuke.stale_openstack_instances(ctx, {
                name: { 'id': 'UUID', },
            }, {
            })
            m['destroy'].assert_not_called()
        #
        # An instance created a very long time ago is destroyed
        #
        with patch.multiple(
                nuke.OpenStackInstance,
                get_created=lambda _: 1000000000,
                __getitem__=lambda _, key: name,
                destroy=DEFAULT,
                ) as m:
            nuke.stale_openstack_instances(ctx, {
                name: { 'id': 'UUID', },
            }, {
                misc.canonicalize_hostname(name, user=None): {},
            })
            m['destroy'].assert_called_with()
        #
        # An instance created but not locked after a while is
        # destroyed.
        #
        with patch.multiple(
                nuke.OpenStackInstance,
                get_created=lambda _: nuke.OPENSTACK_DELAY + 1,
                __getitem__=lambda _, key: name,
                destroy=DEFAULT,
                ) as m:
            nuke.stale_openstack_instances(ctx, {
                name: { 'id': 'UUID', },
            }, {
            })
            m['destroy'].assert_called_with()
        #
        # An instance created within the expected lifetime
        # of a job and locked is left untouched.
        #
        with patch.multiple(
                nuke.OpenStackInstance,
                get_created=lambda _: nuke.OPENSTACK_DELAY + 1,
                __getitem__=lambda _, key: name,
                destroy=DEFAULT,
                ) as m:
            nuke.stale_openstack_instances(ctx, {
                name: { 'id': 'UUID', },
            }, {
                misc.canonicalize_hostname(name, user=None): {},
            })
            m['destroy'].assert_not_called()
