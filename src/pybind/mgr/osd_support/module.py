from typing import List, Set, Optional
from mgr_module import MgrModule, HandleCommandResult
from threading import Event
import json
import errno


class OSDSupport(MgrModule):
    # these are CLI commands we implement
    COMMANDS = [
        {
            "cmd": "osd drain name=osd_ids,type=CephInt,req=true,n=N",
            "desc": "drain osd ids",
            "perm": "r"
        },
        {
            "cmd": "osd drain status",
            "desc": "show status",
            "perm": "r"
        },
        {
            "cmd": "osd drain stop name=osd_ids,type=CephInt,req=false,n=N",
            "desc": "show status for osds. Stopping all if osd_ids are omitted",
            "perm": "r"
        },
    ]

    MODULE_OPTIONS: List[dict] = []

    # These are "native" Ceph options that this module cares about.
    NATIVE_OPTIONS: List[str] = []

    osd_ids: Set[int] = set()
    emptying_osds: Set[int] = set()
    check_osds: Set[int] = set()
    empty: Set[int] = set()

    def __init__(self, *args, **kwargs):
        super(OSDSupport, self).__init__(*args, **kwargs)

        # set up some members to enable the serve() method and shutdown()
        self.run = True
        self.event = Event()

        # ensure config options members are initialized; see config_notify()
        self.config_notify()

    def config_notify(self):
        """
        This method is called whenever one of our config options is changed.
        """
        # This is some boilerplate that stores MODULE_OPTIONS in a class
        # member, so that, for instance, the 'emphatic' option is always
        # available as 'self.emphatic'.
        for opt in self.MODULE_OPTIONS:
            setattr(self,
                    opt['name'],
                    self.get_module_option(opt['name']))
            self.log.debug(' mgr option %s = %s',
                           opt['name'], getattr(self, opt['name']))
        # Do the same for the native options.
        for _opt in self.NATIVE_OPTIONS:
            setattr(self,
                    _opt,
                    self.get_ceph_option(_opt))
            self.log.debug('native option %s = %s', _opt, getattr(self, _opt))

    def handle_command(self, inbuf, cmd):
        ret = 0
        err = ''
        _ = inbuf
        cmd_prefix = cmd.get('prefix', '')
        osd_ids: List[int] = cmd.get('osd_ids', list())
        not_found_osds: Set[int] = self.osds_not_in_cluster(osd_ids)
        if cmd_prefix == 'osd drain':
            if not_found_osds:
                return -errno.EINVAL, '', f"OSDs <{not_found_osds}> not found in cluster"
            # add osd_ids to set
            for osd_id in osd_ids:
                if osd_id not in self.emptying_osds:
                    self.osd_ids.add(osd_id)
            self.log.info(f'Found OSD(s) <{self.osd_ids}> in the queue.')
            out = 'Started draining OSDs. Query progress with <ceph osd drain status>'

        elif cmd_prefix == 'osd drain status':
            # re-initialize it with an empty set on invocation (long running processes)
            self.check_osds = set()
            # assemble a set of emptying osds and to_be_emptied osds
            self.check_osds.update(self.emptying_osds)
            self.check_osds.update(self.osd_ids)
            self.check_osds.update(self.empty)

            report = list()
            for osd_id in self.check_osds:
                pgs = self.get_pg_count(osd_id)
                report.append(dict(osd_id=osd_id, pgs=pgs))
            out = f"{json.dumps(report)}"

        elif cmd_prefix == 'osd drain stop':
            if not osd_ids:
                self.log.debug("No osd_ids provided, stop all pending drain operations)")
                self.osd_ids = set()
                self.emptying_osds = set()

                # this is just a poor-man's solution as it will not really stop draining
                # the osds. It will just stop the queries and also prevent any scheduled OSDs
                # from getting drained at a later point in time.
                out = "Stopped all future draining operations (not resetting the weight for already reweighted OSDs)"

            else:
                if not_found_osds:
                    return -errno.EINVAL, '', f"OSDs <{not_found_osds}> not found in cluster"

                self.osd_ids = self.osd_ids.difference(osd_ids)
                self.emptying_osds = self.emptying_osds.difference(osd_ids)
                out = f"Stopped draining operations for OSD(s): {osd_ids}"

        else:
            return HandleCommandResult(
                retval=-errno.EINVAL,
                stdout='',
                stderr=f"Command not found <{cmd.get('prefix', '')}>")
        return HandleCommandResult(
            retval=ret,   # exit code
            stdout=out,   # stdout
            stderr=err)

    def serve(self):
        """
        This method is called by the mgr when the module starts and can be
        used for any background activity.
        """
        self.log.info("Starting mgr/osd_support")
        while self.run:

            self.log.debug(f"Scheduled for draining: <{self.osd_ids}>")
            self.log.debug(f"Currently being drained: <{self.emptying_osds}>")
            # the state should be saved to the mon store in the actual call and
            # then retrieved in serve() probably

            # 1) check if all provided osds can be stopped, if not, shrink list until ok-to-stop
            for x in self.find_osd_stop_threshold(self.osd_ids):
                self.emptying_osds.add(x)

            # remove the emptying osds from the osd_ids since they don't need to be checked again.
            self.osd_ids = self.osd_ids.difference(self.emptying_osds)

            # 2) reweight the ok-to-stop osds, ONCE
            self.reweight_osds(self.emptying_osds)

            # 3) check for osds to be empty
            empty_osds = self.empty_osds(self.emptying_osds)

            # remove osds that are marked as empty
            self.emptying_osds = self.emptying_osds.difference(empty_osds)

            # move empty osds in the done queue until they disappear from ceph's view
            # other modules need to know when OSDs are empty
            for osd in empty_osds:
                self.log.debug(f"Adding {osd} to list of empty OSDs")
                self.empty.add(osd)

            # remove from queue if no longer part of ceph cluster
            self.cleanup()

            # fixed sleep interval of 10 seconds
            sleep_interval = 10
            self.log.debug('Sleeping for %d seconds', sleep_interval)
            self.event.wait(sleep_interval)
            self.event.clear()

    def cleanup(self):
        """
        Remove OSDs that are no longer in the ceph cluster from the
        'done' list.
        :return:
        """
        for osd in self.osds_not_in_cluster(list(self.empty)):
            self.log.info(f"OSD: {osd} is not found in the cluster anymore. Removing")
            self.empty.remove(osd)

    def shutdown(self):
        """
        This method is called by the mgr when the module needs to shut
        down (i.e., when the serve() function needs to exit).
        """
        self.log.info('Stopping')
        self.run = False
        self.event.set()

    def get_osds_in_cluster(self) -> List[str]:
        osd_map = self.get_osdmap()
        return [x.get('osd') for x in osd_map.dump().get('osds', [])]

    def osds_not_in_cluster(self, osd_ids: List[int]) -> Set[int]:
        self.log.info(f"Checking if provided osds <{osd_ids}> exist in the cluster")
        cluster_osds = self.get_osds_in_cluster()
        not_in_cluster = set()
        for osd_id in osd_ids:
            if int(osd_id) not in cluster_osds:
                self.log.error(f"Could not find {osd_id} in cluster")
                not_in_cluster.add(osd_id)
        return not_in_cluster

    def empty_osds(self, osd_ids: Set[int]) -> List[int]:
        if not osd_ids:
            return list()
        osd_df_data = self.osd_df()
        empty_osds = list()
        for osd_id in osd_ids:
            if self.is_empty(osd_id, osd_df=osd_df_data):
                empty_osds.append(osd_id)
        return empty_osds

    def osd_df(self) -> dict:
        # TODO: this should be cached I think
        base_cmd = 'osd df'
        ret, out, err = self.mon_command({
            'prefix': base_cmd,
            'format': 'json'
        })
        return json.loads(out)

    def is_empty(self, osd_id: int, osd_df: Optional[dict] = None) -> bool:
        pgs = self.get_pg_count(osd_id, osd_df=osd_df)
        if pgs != 0:
            self.log.info(f"osd: {osd_id} still has {pgs} PGs.")
            return False
        self.log.info(f"osd: {osd_id} has no PGs anymore")
        return True

    def reweight_osds(self, osd_ids: Set[int]) -> bool:
        results = [(self.reweight_osd(osd_id)) for osd_id in osd_ids]
        return all(results)

    def get_pg_count(self, osd_id: int, osd_df: Optional[dict] = None) -> int:
        if not osd_df:
            osd_df = self.osd_df()
        osd_nodes = osd_df.get('nodes', [])
        for osd_node in osd_nodes:
            if osd_node.get('id') == int(osd_id):
                return osd_node.get('pgs', -1)
        return -1

    def get_osd_weight(self, osd_id: int) -> float:
        osd_df = self.osd_df()
        osd_nodes = osd_df.get('nodes', [])
        for osd_node in osd_nodes:
            if osd_node.get('id') == int(osd_id):
                if 'crush_weight' in osd_node:
                    return float(osd_node.get('crush_weight'))
        return -1.0

    def reweight_osd(self, osd_id: int, weight: float = 0.0) -> bool:
        if self.get_osd_weight(osd_id) == weight:
            self.log.debug(f"OSD <{osd_id}> is already weighted with: {weight}")
            return True
        base_cmd = 'osd crush reweight'
        ret, out, err = self.mon_command({
            'prefix': base_cmd,
            'name': f'osd.{osd_id}',
            'weight': weight
        })
        cmd = f"{base_cmd} on osd.{osd_id} to weight: {weight}"
        self.log.debug(f"running cmd: {cmd}")
        if ret != 0:
            self.log.error(f"command: {cmd} failed with: {err}")
            return False
        self.log.info(f"command: {cmd} succeeded with: {out}")
        return True

    def find_osd_stop_threshold(self, osd_ids: Set[int]) -> Set[int]:
        """
        Cut osd_id list in half until it's ok-to-stop

        :param osd_ids: list of osd_ids
        :return: list of ods_ids that can be stopped at once
        """
        if not osd_ids:
            return set()
        _osds: List[int] = list(osd_ids.copy())
        while not self.ok_to_stop(_osds):
            if len(_osds) <= 1:
                # can't even stop one OSD, aborting
                self.log.info("Can't even stop one OSD. Cluster is probably busy. Retrying later..")
                return set()
            self.event.wait(1)
            # splitting osd_ids in half until ok_to_stop yields success
            # maybe popping ids off one by one is better here..depends on the cluster size I guess..
            # There's a lot of room for micro adjustments here
            _osds = _osds[len(_osds)//2:]
        return set(_osds)

    def ok_to_stop(self, osd_ids: List[int]) -> bool:
        base_cmd = "osd ok-to-stop"
        self.log.debug(f"running cmd: {base_cmd} on ids {osd_ids}")
        ret, out, err = self.mon_command({
            'prefix': base_cmd,
            # apparently ok-to-stop allows strings only
            'ids': [str(x) for x in osd_ids]
        })
        if ret != 0:
            self.log.error(f"{osd_ids} are not ok-to-stop. {err}")
            return False
        self.log.info(f"OSDs <{osd_ids}> are ok-to-stop")
        return True
