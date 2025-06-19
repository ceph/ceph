import json
import time
import errno
from logging import getLogger
from io import StringIO

from tasks.cephfs.test_volumes import TestVolumesHelper

from teuthology.contextutil import safe_while
from teuthology.exceptions import CommandFailedError

log = getLogger(__name__)


class RsizeDoesntMatch(Exception):

    def __init__(self, msg):
        self.msg = msg


class CloneProgressReporterHelper(TestVolumesHelper):
    CLIENTS_REQUIRED = 1

    def setUp(self):
        super(CloneProgressReporterHelper, self).setUp()

        # save this config value so that it can be set again at the end of test
        # and therefore other tests that might depend on this won't be
        # disturbed unnecessarily.
        self.num_of_cloner_threads_def = self.get_ceph_cmd_stdout(
            'config get mgr mgr/volumes/max_concurrent_clones').strip()

        # set number of cloner threads to 4, tests in this class depend on this.
        self.run_ceph_cmd('config set mgr mgr/volumes/max_concurrent_clones 4')

    def tearDown(self):
        v = self.volname
        o = self.get_ceph_cmd_stdout('fs volume ls')
        if self.volname not in o:
            super(CloneProgressReporterHelper, self).tearDown()
            return

        subvols = self.get_ceph_cmd_stdout(f'fs subvolume ls {v} --format '
                                           'json')
        subvols = json.loads(subvols)
        for i in subvols:
            sv = tuple(i.values())[0]
            if 'clone' in sv:
                self.run_ceph_cmd(f'fs subvolume rm --force {v} {sv}')
                continue

            p = self.run_ceph_cmd(f'fs subvolume snapshot ls {v} {sv} '
                                   '--format json', stdout=StringIO())
            snaps = p.stdout.getvalue().strip()
            snaps = json.loads(snaps)
            for j in snaps:
                ss = tuple(j.values())[0]
                self.run_ceph_cmd('fs subvolume snapshot rm --force '
                                  f'--format json {v} {sv} {ss}')

            try:
                self.run_ceph_cmd(f'fs subvolume rm {v} {sv}')
            except CommandFailedError as e:
                if e.exitstatus == errno.ENOENT:
                    log.info(
                        'ignoring this error, perhaps subvolume was deleted '
                        'during the test and snapshot deleted above is a '
                        'retained snapshot. when a retained snapshot (which is '
                        'snapshot retained despite of subvolume deletion) is '
                        'deleted, the subvolume directory is also deleted '
                        'along. and before retained snapshot deletion, the '
                        'subvolume is reported by "subvolume ls" command, which'
                        'is what probably caused confusion here')
                    pass
                else:
                    raise

        # verify trash dir is clean
        self._wait_for_trash_empty()

        self.run_ceph_cmd('config set mgr mgr/volumes/max_concurrent_clones '
                          f'{self.num_of_cloner_threads_def}')

        # this doesn't work as expected because cleanup is not done when a
        # volume is deleted.
        #
        # delete volumes so that all async purge threads, async cloner
        # threads, progress bars, etc. associated with it are removed from
        # Ceph cluster.
        #self.run_ceph_cmd(f'fs volume rm {self.volname} --yes-i-really-mean-it')

        super(CloneProgressReporterHelper, self).tearDown()

    # XXX: it is important to wait for rbytes value to catch up to actual size of
    # subvolume so that progress bar shows sensible amount of progress
    def wait_till_rbytes_is_right(self, v_name, sv_name, exp_size,
                                  grp_name=None, sleep=2, max_count=60):
        getpath_cmd = f'fs subvolume getpath {v_name} {sv_name}'
        if grp_name:
            getpath_cmd += f' {grp_name}'
        sv_path = self.get_ceph_cmd_stdout(getpath_cmd)
        sv_path = sv_path[1:]

        for i in range(max_count):
            r_size = self.mount_a.get_shell_stdout(
                f'getfattr -n ceph.dir.rbytes {sv_path}').split('rbytes=')[1]
            r_size = int(r_size.replace('"', '').replace('"', ''))
            log.info(f'r_size = {r_size} exp_size = {exp_size}')
            if exp_size == r_size:
                break

            time.sleep(sleep)
        else:
            msg = ('size reported by rstat is not the expected size.\n'
                   f'expected size = {exp_size}\n'
                   f'size reported by rstat = {r_size}')
            raise RsizeDoesntMatch(msg)

    def filter_in_only_clone_pevs(self, progress_events):
        '''
        Progress events dictionary in output of "ceph status --format json"
        has the progress bars and message associated with each progress bar.
        Sometimes during testing of clone progress bars, and sometimes
        otherwise too, an extra progress bar is seen with message "Global
        Recovery Event". This extra progress bar interferes with testing of
        progress bars for cloning.

        This helper methods goes through this dictionary and picks only
        (filters in) clone events.
        '''
        clone_pevs = {}

        for k, v in progress_events.items():
            if 'mgr-vol-ongoing-clones' in k or 'mgr-vol-total-clones' in k:
                clone_pevs[k] = v

        return clone_pevs

    def get_pevs_from_ceph_status(self, clones=None, check=True):
        o = self.get_ceph_cmd_stdout('status --format json-pretty')
        o = json.loads(o)

        try:
            pevs = o['progress_events'] # pevs = progress events
        except KeyError as e:
            try:
                if check and clones:
                    self.__check_clone_state('completed', clone=clones, timo=1)
            except:
                msg = ('Didn\'t find expected entries in dictionary '
                       '"progress_events" which is obtained from the '
                       'output of command "ceph status".\n'
                       f'Exception - {e}\npev -\n{pevs}')
                raise Exception(msg)

        pevs = self.filter_in_only_clone_pevs(pevs)

        return pevs

    def wait_for_both_progress_bars_to_appear(self, sleep=1, iters=20):
        pevs = []
        msg = (f'Waited for {iters*sleep} seconds but couldn\'t 2 progress '
                'bars in output of "ceph status" command.')
        with safe_while(tries=iters, sleep=sleep, action=msg) as proceed:
            while proceed():
                o = self.get_ceph_cmd_stdout('status --format json-pretty')
                o = json.loads(o)
                pevs = o['progress_events']
                pevs = self.filter_in_only_clone_pevs(pevs)
                if len(pevs) == 2:
                    v = tuple(pevs.values())
                    if 'ongoing+pending' in v[1]['message']:
                        self.assertIn('ongoing', v[0]['message'])
                    else:
                        self.assertIn('ongoing', v[1]['message'])
                        self.assertIn('ongoing+pending', v[0]['message'])
                    break

    def get_onpen_count(self, pev):
        '''
        Return number of clones reported in the message of progress bar for
        ongoing+pending clones.
        '''
        i = pev['message'].find('ongoing+pending')
        if i == -1:
            return
        count = pev['message'][:i]
        count = count[:-1] # remomve trailing space
        count = int(count)
        return count

    def get_both_progress_fractions_and_onpen_count(self):
        '''
        Go through output of "ceph status --format json-pretty" and return
        progress made by both clones (that is progress fractions) and return
        number of clones in reported in message of ongoing+pending progress
        bar.
        '''
        msg = 'Expected 2 progress bars but found ' # rest continued in loop
        with safe_while(tries=20, sleep=1, action=msg) as proceed:
            while proceed():
                o = self.get_ceph_cmd_stdout('status --format json-pretty')
                o = json.loads(o)
                pevs = o['progress_events']
                pevs = self.filter_in_only_clone_pevs(pevs)
                if len(pevs.values()) == 2:
                    break
                else:
                    msg += f'{len(pevs)} instead'

        log.info(f'pevs -\n{pevs}')
        # on_p - progress fraction for ongoing clone jobs
        # onpen_p - progress fraction for ongoing+pending clone jobs
        pev1, pev2 = tuple(pevs.values())
        if 'ongoing+pending' in pev1['message']:
            onpen_p = pev1['progress']
            onpen_count = self.get_onpen_count(pev1)
            on_p = pev2['progress']
        else:
            onpen_p = pev2['progress']
            onpen_count = self.get_onpen_count(pev2)
            on_p = pev1['progress']

        on_p = float(on_p)
        onpen_p = float(onpen_p)

        return on_p, onpen_p, onpen_count

    # "ceph fs clone cancel" command takes considerable time to finish running.
    # test cases where more than 4 clones are being cancelled, this error is
    # seen, and can be safely ignored since it only implies that cloning has
    # been finished.
    def cancel_clones_and_ignore_if_finished(self, clones):
        if isinstance(clones, str):
            clones = (clones, )

        for c in clones:
            cmdargs = f'fs clone cancel {self.volname} {c}'
            proc = self.run_ceph_cmd(args=cmdargs, stderr=StringIO(),
                                     check_status=False)

            stderr = proc.stderr.getvalue().strip().lower()
            if proc.exitstatus == 0:
                continue
            elif proc.exitstatus == 22 and 'clone finished' in stderr:
                continue
            else:
                cmdargs = './bin/ceph ' + cmdargs
                raise CommandFailedError(cmdargs, proc.exitstatus)

    def cancel_clones(self, clones, check_status=True):
        v = self.volname
        if not isinstance(clones, (tuple, list)):
            clones = (clones, )

        for i in clones:
            self.run_ceph_cmd(f'fs clone cancel {v} {i}',
                               check_status=check_status)
            time.sleep(2)

    # check status is False since this method is meant to cleanup clones at
    # the end of a test case and some clones might already be complete.
    def cancel_clones_and_confirm(self, clones, check_status=False):
        if not isinstance(clones, (tuple, list)):
            clones = (clones, )

        self.cancel_clones(clones, check_status)

        for i in clones:
            self._wait_for_clone_to_be_canceled(i)

    def cancel_clones_and_assert(self, clones):
        v = self.volname
        if not isinstance(clones, (tuple, list)):
            clones = (clones, )

        self.cancel_clones(clones, True)

        for i in clones:
            o = self.get_ceph_cmd_stdout(f'fs clone status {v} {i}')
            try:
                self.assertIn('canceled', o)
            except AssertionError:
                self.assertIn('complete', o)

    def _wait_for_clone_progress_bars_to_be_removed(self):
        with safe_while(tries=10, sleep=0.5) as proceed:
            while proceed():
                o = self.get_ceph_cmd_stdout('status --format json-pretty')
                o = json.loads(o)

                pevs = o['progress_events'] # pevs = progress events
                pevs = self.filter_in_only_clone_pevs(pevs)
                if not pevs:
                    break


# NOTE: these tests consumes considerable amount of CPU and RAM due generation
# random of files and due to multiple cloning jobs that are run simultaneously.
#
# NOTE: mgr/vol code generates progress bars for cloning jobs and these tests
# capture them through "ceph status --format json-pretty" and checks if they
# are as expected. If cloning happens too fast, these tests will fail to
# capture progress bars, at least in desired state. Thus, these tests are
# slightly racy by their very nature.
#
# Two measure can be taken to avoid this (and thereby inconsistent results in
# testing) -
# 1. Slow down cloning. This was done by adding a sleep after every file is
# copied. However, this method was rejected since a new config for this would
# have to be added.
# 2. Amount of data that will cloned is big enough so that cloning takes enough
# time for test code to capture the progress bar in desired state and finish
# running. This is method that has been currently employed. This consumes
# significantly more time, CPU and RAM in comparison.
class TestCloneProgressReporter(CloneProgressReporterHelper):
    '''
    This class contains tests for features that show how much progress cloning
    jobs have made.
    '''


    def test_progress_is_printed_in_clone_status_output(self):
        '''
        Test that the command "ceph fs clone status" prints progress stats
        for the clone.
        '''
        v = self.volname
        sv = 'sv1'
        ss = 'ss1'
        # "clone" must be part of clone name for sake of tearDown()
        c = 'ss1clone1'

        self.run_ceph_cmd(f'fs subvolume create {v} {sv} --mode=777')
        size = self._do_subvolume_io(sv, None, None, 3, 1024)

        self.run_ceph_cmd(f'fs subvolume snapshot create {v} {sv} {ss}')
        self.wait_till_rbytes_is_right(v, sv, size)

        self.run_ceph_cmd(f'fs subvolume snapshot clone {v} {sv} {ss} {c}')
        self._wait_for_clone_to_be_in_progress(c)

        with safe_while(tries=120, sleep=1) as proceed:
            while proceed():
                o = self.get_ceph_cmd_stdout(f'fs clone status {v} {c}')
                o = json.loads(o)

                try:
                    p = o['status']['progress_report']['percentage cloned']
                    log.debug(f'percentage cloned = {p}')
                except KeyError:
                    # if KeyError is caught, either progress_report is present
                    # or clone is complete
                    if 'progress_report' in ['status']:
                        self.assertEqual(o['status']['state'], 'complete')
                    break

        self._wait_for_clone_to_complete(c)

    def test_clones_less_than_cloner_threads(self):
        '''
        Test that one progress bar is printed in output of "ceph status" output
        when number of clone jobs is less than number of cloner threads.
        '''
        v = self.volname
        sv = 'sv1'
        ss = 'ss1'
        # XXX: "clone" must be part of clone name for sake of tearDown()
        c = 'ss1clone1'

        self.run_ceph_cmd(f'fs subvolume create {v} {sv} --mode=777')
        size = self._do_subvolume_io(sv, None, None, 10, 1024)

        self.run_ceph_cmd(f'fs subvolume snapshot create {v} {sv} {ss}')
        self.wait_till_rbytes_is_right(v, sv, size)

        self.run_ceph_cmd(f'fs subvolume snapshot clone {v} {sv} {ss} {c}')

        with safe_while(tries=10, sleep=1) as proceed:
            while proceed():
                pev = self.get_pevs_from_ceph_status(c)

                if len(pev) < 1:
                   continue
                elif len(pev) > 1:
                    raise RuntimeError('For 1 clone "ceph status" output has 2 '
                                       'progress bars, it should have only 1 '
                                       f'progress bar.\npev -\n{pev}')

                # ensure that exactly 1 progress bar for cloning is present in
                # "ceph status" output
                msg = ('"progress_events" dict in "ceph status" output must have '
                       f'exactly one entry.\nprogress_event dict -\n{pev}')
                self.assertEqual(len(pev), 1, msg)

                pev_msg = tuple(pev.values())[0]['message']
                self.assertIn('1 ongoing clones', pev_msg)
                break

        # allowing clone jobs to finish will consume too much time and space
        # and not cancelling these clone doesnt affect this test case.
        self.cancel_clones_and_ignore_if_finished(c)

    def test_clone_to_diff_group_and_less_than_cloner_threads(self):
        '''
        Initiate cloning where clone subvolume and source subvolume are located
        in different groups and then test that when this clone is in progress,
        one progress bar is printed in output of command "ceph status" that
        shows progress of this clone.
        '''
        v = self.volname
        group = 'group1'
        sv = 'sv1'
        ss = 'ss1'
        # XXX: "clone" must be part of clone name for sake of tearDown()
        c = 'ss1clone1'

        self.run_ceph_cmd(f'fs subvolumegroup create {v} {group}')
        self.run_ceph_cmd(f'fs subvolume create {v} {sv} {group} --mode=777')
        size = self._do_subvolume_io(sv, group, None, 10, 1024)

        self.run_ceph_cmd(f'fs subvolume snapshot create {v} {sv} {ss} {group}')
        self.wait_till_rbytes_is_right(v, sv, size, group)

        self.run_ceph_cmd(f'fs subvolume snapshot clone {v} {sv} {ss} {c} '
                          f'--group-name {group}')

        with safe_while(tries=10, sleep=1) as proceed:
            while proceed():
                pev = self.get_pevs_from_ceph_status(c)

                if len(pev) < 1:
                   continue
                elif len(pev) > 1:
                    raise RuntimeError('For 1 clone "ceph status" output has 2 '
                                       'progress bars, it should have only 1 '
                                       f'progress bar.\npev -\n{pev}')

                # ensure that exactly 1 progress bar for cloning is present in
                # "ceph status" output
                msg = ('"progress_events" dict in "ceph status" output must have '
                       f'exactly one entry.\nprogress_event dict -\n{pev}')
                self.assertEqual(len(pev), 1, msg)

                pev_msg = tuple(pev.values())[0]['message']
                self.assertIn('1 ongoing clones', pev_msg)
                break

        # allowing clone jobs to finish will consume too much time and space
        # and not cancelling these clone doesnt affect this test case.
        self.cancel_clones_and_ignore_if_finished(c)

    def test_clone_after_subvol_is_removed(self):
        '''
        Initiate cloning after source subvolume has been deleted but with
        snapshots retained and then test that, when this clone is in progress,
        one progress bar is printed in output of command "ceph status" that
        shows progress of this clone.
        '''
        v = self.volname
        sv = 'sv1'
        ss = 'ss1'
        # XXX: "clone" must be part of clone name for sake of tearDown()
        c = 'ss1clone1'

        # XXX: without setting mds_snap_rstat to true rstats are not updated on
        # a subvolume snapshot and therefore clone progress bar will not show
        # any progress.
        self.config_set('mds', 'mds_snap_rstat', 'true')

        self.run_ceph_cmd(f'fs subvolume create {v} {sv} --mode=777')
        size = self._do_subvolume_io(sv, None, None, 10, 1024)

        self.run_ceph_cmd(f'fs subvolume snapshot create {v} {sv} {ss}')
        self.wait_till_rbytes_is_right(v, sv, size)

        self.run_ceph_cmd(f'fs subvolume rm {v} {sv} --retain-snapshots')
        self.run_ceph_cmd(f'fs subvolume snapshot clone {v} {sv} {ss} {c}')

        with safe_while(tries=15, sleep=10) as proceed:
            while proceed():
                pev = self.get_pevs_from_ceph_status(c)

                if len(pev) < 1:
                   continue
                elif len(pev) > 1:
                    raise RuntimeError('For 1 clone "ceph status" output has 2 '
                                       'progress bars, it should have only 1 '
                                       f'progress bar.\npev -\n{pev}')

                # ensure that exactly 1 progress bar for cloning is present in
                # "ceph status" output
                msg = ('"progress_events" dict in "ceph status" output must have '
                       f'exactly one entry.\nprogress_event dict -\n{pev}')
                self.assertEqual(len(pev), 1, msg)

                pev_msg = tuple(pev.values())[0]['message']
                self.assertIn('1 ongoing clones', pev_msg)
                break

        # allowing clone jobs to finish will consume too much time and space
        # and not cancelling these clone doesnt affect this test case.
        self.cancel_clones_and_ignore_if_finished(c)

    def test_clones_equal_to_cloner_threads(self):
        '''
        Test that one progress bar is printed in output of "ceph status" output
        when number of clone jobs is equal to number of cloner threads.
        '''
        v = self.volname
        sv = 'sv1'
        ss = 'ss1'
        c = self._gen_subvol_clone_name(4)

        self.run_ceph_cmd(f'fs subvolume create {v} {sv} --mode=777')
        size = self._do_subvolume_io(sv, None, None, 10, 1024)

        self.run_ceph_cmd(f'fs subvolume snapshot create {v} {sv} {ss}')
        self.wait_till_rbytes_is_right(v, sv, size)

        for i in c:
            self.run_ceph_cmd(f'fs subvolume snapshot clone {v} {sv} {ss} {i}')

        with safe_while(tries=10, sleep=1) as proceed:
            while proceed():
                pev = self.get_pevs_from_ceph_status(c)

                if len(pev) < 1:
                    time.sleep(1)
                    continue
                elif len(pev) > 1:
                    raise RuntimeError('For 1 clone "ceph status" output has 2 '
                                       'progress bars, it should have only 1 '
                                       f'progress bar.\npev -\n{pev}')

                # ensure that exactly 1 progress bar for cloning is present in
                # "ceph status" output
                msg = ('"progress_events" dict in "ceph status" output must have '
                       f'exactly one entry.\nprogress_event dict -\n{pev}')
                self.assertEqual(len(pev), 1, msg)

                pev_msg = tuple(pev.values())[0]['message']
                self.assertIn('ongoing clones', pev_msg)
                break

        # allowing clone jobs to finish will consume too much time and space
        # and not cancelling these clone doesnt affect this test case.
        self.cancel_clones_and_ignore_if_finished(c)

    def test_clones_more_than_cloner_threads(self):
        '''
        Test that 2 progress bars are printed in output of "ceph status"
        command when number of clone jobs is greater than number of cloner
        threads.

        Also, test that one of these progress bars is for ongoing clones and
        other progress bar for ongoing+pending clones.
        '''
        v = self.volname
        sv = 'sv1'
        ss = 'ss1'
        c = self._gen_subvol_clone_name(7)

        self.config_set('mgr', 'mgr/volumes/snapshot_clone_no_wait', 'false')
        self.run_ceph_cmd(f'fs subvolume create {v} {sv} --mode=777')
        size = self._do_subvolume_io(sv, None, None, 3, 1024)

        self.run_ceph_cmd(f'fs subvolume snapshot create {v} {sv} {ss}')
        self.wait_till_rbytes_is_right(v, sv, size)

        for i in c:
            self.run_ceph_cmd(f'fs subvolume snapshot clone {v} {sv} {ss} {i}')

        msg = ('messages for progress bars for snapshot cloning are not how '
               'they were expected')
        with safe_while(tries=20, sleep=1, action=msg) as proceed:
            while proceed():
                pevs = self.get_pevs_from_ceph_status(c)

                if len(pevs) <= 1:
                    continue # let's wait for second progress bar to appear
                elif len(pevs) > 2:
                    raise RuntimeError(
                        'More than 2 progress bars were found in the output '
                        'of "ceph status" command.\nprogress events -'
                        f'\n{pevs}')

                msg = ('"progress_events" dict in "ceph -s" output must have '
                       f'only two entries.\n{pevs}')
                self.assertEqual(len(pevs), 2, msg)
                pev1, pev2 = pevs.values()
                if ('ongoing clones' in pev1['message'].lower() and
                    'total ' in pev2['message'].lower()):
                    break
                elif ('ongoing clones' in pev2['message'].lower() or
                    'total ' in pev1['message'].lower()):
                    break
                else:
                    raise RuntimeError(msg)

        # allowing clone jobs to finish will consume too much time, space and
        # CPU and not cancelling these clone doesnt affect this test case.
        self.cancel_clones_and_ignore_if_finished(c)

    def test_progress_drops_when_new_jobs_are_added(self):
        '''
        Test that progress indicated by progress bar for ongoing+pending clones
        drops when more clone jobs are launched.
        '''
        v = self.volname
        sv = 'sv1'
        ss = 'ss1'
        c = self._gen_subvol_clone_name(20)

        self.config_set('mgr', 'mgr/volumes/snapshot_clone_no_wait', 'false')
        self.run_ceph_cmd(f'fs subvolume create {v} {sv} --mode=777')
        size = self._do_subvolume_io(sv, None, None, 3, 1024)

        self.run_ceph_cmd(f'fs subvolume snapshot create {v} {sv} {ss}')
        self.wait_till_rbytes_is_right(v, sv, size)

        for i in c[:5]:
            self.run_ceph_cmd(f'fs subvolume snapshot clone {v} {sv} {ss} {i}')

        tuple_ = self.get_both_progress_fractions_and_onpen_count()
        if isinstance(tuple_, (list, tuple)) and len(tuple_) == 3:
            on_p, onpen_p, onpen_count = tuple_

        # this should cause onpen progress bar to go back
        for i in c[5:]:
            self.run_ceph_cmd(f'fs subvolume snapshot clone {v} {sv} {ss} {i}')
        time.sleep(2)

        with safe_while(tries=30, sleep=0.5) as proceed:
            while proceed():
                tuple_ = self.get_both_progress_fractions_and_onpen_count()
                new_on_p, new_onpen_p, new_onpen_count = tuple_
                if new_onpen_p < onpen_p:
                    log.info('new_onpen_p is less than onpen_p.')
                    log.info(f'new_onpen_p = {new_onpen_p}; onpen_p = {onpen_p}')
                    break
                log.info(f'on_p = {on_p} new_on_p = {new_on_p}')
                log.info(f'onpen_p = {onpen_p} new_onpen_p = {new_onpen_p}')
                log.info(f'onpen_count = {onpen_count} new_onpen_count = '
                         f'{new_onpen_count}')
            else:
                self.cancel_clones_and_ignore_if_finished(c)
                raise RuntimeError('Test failed: it was expected for '
                                   '"new_onpen_p < onpen_p" to be true.')

        # average progress for "ongoing + pending" clone jobs must
        # reduce since a new job was added to penidng state
        self.assertLess(new_onpen_p, onpen_p)

        # allowing clone jobs to finish will consume too much time and space
        # and not cancelling these clone doesnt affect this test case.
        self.cancel_clones_and_ignore_if_finished(c)

    def test_when_clones_cancelled_are_less_than_cloner_threads(self):
        '''
        Test that the progress bar that is printed for 1 ongoing clone job is
        removed from the output of "ceph status" command when a clone is
        cancelled.
        '''
        v = self.volname
        sv = 'sv1'
        ss = 'ss1'
        # "clone" must be part of clone name for sake of tearDown()
        c = 'ss1clone1'

        self.run_ceph_cmd(f'fs subvolume create {v} {sv} --mode=777')

        sv_path = self.get_ceph_cmd_stdout(f'fs subvolume getpath {v} {sv}')
        sv_path = sv_path[1:]

        size = self._do_subvolume_io(sv, None, None, 3, 1024)
        self.run_ceph_cmd(f'fs subvolume snapshot create {v} {sv} {ss}')
        self.wait_till_rbytes_is_right(v, sv, size)

        self.run_ceph_cmd(f'fs subvolume snapshot clone {v} {sv} {ss} {c}')
        time.sleep(1)
        self.cancel_clones_and_ignore_if_finished(c)
        self._wait_for_clone_to_be_canceled(c)
        self._wait_for_clone_progress_bars_to_be_removed()

        # test that cloning had begun but didn't finish.
        try:
            sv_path = sv_path.replace(sv, c)
            o = self.mount_a.run_shell(f'ls -lh {sv_path}')
            o = o.stdout.getvalue().strip()
            # ensure that all files were not copied. 'ls -lh' will print 1 file
            # per line with an extra line for  summary, so this command must
            # print less than 4 lines
            self.assertLess(len(o.split('\n')), 4)
        except CommandFailedError as cfe:
            # if command failed due to errno 2 (no such file or dir), this
            # means cloning hadn't begun yet. that too is fine
            if cfe.exitstatus == 2:
                pass
            else:
                raise

    def test_when_clones_cancelled_are_equal_to_cloner_threads(self):
        '''
        Test that progress bars, that printed for 3 ongoing clone jobs, are
        removed from the output of "ceph status" command when all 3 clone jobs
        are cancelled.
        '''
        v = self.volname
        sv = 'sv1'
        ss = 'ss1'
        c = self._gen_subvol_clone_name(3)

        self.run_ceph_cmd(f'fs subvolume create {v} {sv} --mode=777')

        sv_path = self.get_ceph_cmd_stdout(f'fs subvolume getpath {v} {sv}')
        sv_path = sv_path[1:]

        size = self._do_subvolume_io(sv, None, None, 3, 1024)
        self.run_ceph_cmd(f'fs subvolume snapshot create {v} {sv} {ss}')
        self.wait_till_rbytes_is_right(v, sv, size)

        for i in c:
            self.run_ceph_cmd(f'fs subvolume snapshot clone {v} {sv} {ss} {i}')
        time.sleep(1)
        self.cancel_clones_and_ignore_if_finished(c)
        for i in c:
            self._wait_for_clone_to_be_canceled(i)
        self._wait_for_clone_progress_bars_to_be_removed()

        try:
            sv_path = sv_path.replace(sv, c[0])
            o = self.mount_a.run_shell(f'ls -lh {sv_path}')
            o = o.stdout.getvalue().strip()
            log.info(o)
            # ensure that all files were not copied. 'ls -lh' will print 1 file
            # per line with an extra line for  summary, so this command must
            # print less than 4 lines
            self.assertLess(len(o.split('\n')), 4)
        except CommandFailedError as cfe:
            # if command failed due to errno 2 (no such file or dir), this
            # means cloning hadn't begun yet. that too is fine
            if cfe.exitstatus == errno.ENOENT:
                pass
            else:
                raise

    def test_when_clones_cancelled_are_more_than_cloner_threads(self):
        '''
        Test that both the progress bars, that are printed for all 7 clone
        jobs, are removed from the output of "ceph status" command when all
        these clones are cancelled.
        '''
        v = self.volname
        sv = 'sv1'
        ss = 'ss1'
        c = self._gen_subvol_clone_name(7)

        self.config_set('mgr', 'mgr/volumes/snapshot_clone_no_wait', 'false')

        self.run_ceph_cmd(f'fs subvolume create {v} {sv} --mode=777')

        sv_path = self.get_ceph_cmd_stdout(f'fs subvolume getpath {v} {sv}')
        sv_path = sv_path[1:]

        size = self._do_subvolume_io(sv, None, None, 3, 1024)
        self.run_ceph_cmd(f'fs subvolume snapshot create {v} {sv} {ss}')
        self.wait_till_rbytes_is_right(v, sv, size)

        for i in c:
            self.run_ceph_cmd(f'fs subvolume snapshot clone {v} {sv} {ss} {i}')
        time.sleep(1)
        self.cancel_clones_and_ignore_if_finished(c)
        for i in c:
            self._wait_for_clone_to_be_canceled(i)
        self._wait_for_clone_progress_bars_to_be_removed()

        try:
            sv_path = sv_path.replace(sv, c[0])
            o = self.mount_a.run_shell(f'ls -lh {sv_path}')
            o = o.stdout.getvalue().strip()
            log.info(o)
            # ensure that all files were not copied. 'ls -lh' will print 1 file
            # per line with an extra line for  summary, so this command must
            # print less than 4 lines
            self.assertLess(len(o.split('\n')), 4)
        except CommandFailedError as cfe:
            # if command failed due to errno 2 (no such file or dir), this
            # means cloning hadn't begun yet. that too is fine
            if cfe.exitstatus == errno.ENOENT:
                pass
            else:
                raise


class TestOngoingClonesCounter(CloneProgressReporterHelper):
    '''
    Class CloneProgressReporter contains the code that lets it figure out the
    number of ongoing clones on its own, without referring the MGR config
    option mgr/volumes/max_concurrenr_clones. This class contains tests to
    ensure that this code, that does the figuring out, is working fine.
    '''

    def _run_test(self, MAX_THREADS, NUM_OF_CLONES):
        v = self.volname
        sv = 'sv1'
        ss = 'ss1'
        c = self._gen_subvol_clone_name(NUM_OF_CLONES)

        self.config_set('mgr', 'mgr/volumes/snapshot_clone_no_wait', 'false')
        self.config_set('mgr', 'mgr/volumes/max_concurrent_clones', MAX_THREADS)
        self.run_ceph_cmd(f'fs subvolume create {v} {sv} --mode=777')

        sv_path = self.get_ceph_cmd_stdout(f'fs subvolume getpath {v} {sv}')
        sv_path = sv_path[1:]

        size = self._do_subvolume_io(sv, None, None, 3, 1024)
        self.run_ceph_cmd(f'fs subvolume snapshot create {v} {sv} {ss}')
        self.wait_till_rbytes_is_right(v, sv, size)

        for i in c:
            self.run_ceph_cmd(f'fs subvolume snapshot clone {v} {sv} {ss} {i}')

        msg = ('messages for progress bars for snapshot cloning are not how '
               'they were expected')
        with safe_while(tries=20, sleep=1, action=msg) as proceed:
            while proceed():
                pevs = self.get_pevs_from_ceph_status(c)

                if len(pevs) <= 1:
                    continue # let's wait for second progress bar to appear
                elif len(pevs) > 2:
                    raise RuntimeError(
                        'More than 2 progress bars were found in the output '
                        'of "ceph status" command.\nprogress events -'
                        f'\n{pevs}')

                msg = ('"progress_events" dict in "ceph -s" output must have '
                       f'only two entries.\n{pevs}')
                self.assertEqual(len(pevs), 2, msg)
                pev1, pev2 = pevs.values()
                pev1_msg, pev2_msg = pev1['message'].lower(), pev2['message'].lower()
                if 'ongoing clones' in pev1_msg and 'total ' in pev2_msg:
                    if f'{MAX_THREADS} ongoing clones' in pev1_msg:
                        break
                elif 'ongoing clones' in pev2_msg and 'total ' in pev1_msg:
                    if f'{MAX_THREADS} ongoing clones' in pev2_msg:
                        break
                else:
                    raise RuntimeError(msg)

        self.cancel_clones_and_ignore_if_finished(c)
        for i in c:
            self._wait_for_clone_to_be_canceled(i)
        self._wait_for_clone_progress_bars_to_be_removed()

    def test_for_2_ongoing_clones(self):
        self._run_test(MAX_THREADS=2, NUM_OF_CLONES=5)

    def test_for_4_ongoing_clones(self):
        self._run_test(MAX_THREADS=4, NUM_OF_CLONES=8)

    def test_for_6_ongoing_clones(self):
        self._run_test(MAX_THREADS=6, NUM_OF_CLONES=16)
