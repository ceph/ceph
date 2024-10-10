from mgr_util import CephfsClient, open_filesystem
from .exception import MDSPartException
import errno
import json
import logging
from .util import get_max_mds

log = logging.getLogger(__name__)

class Subtree:
    def __init__(self, mgr):
        log.debug("Init subtree module.")
        self.mgr = mgr
        self.rados = mgr.rados
        self.fs_map = self.mgr.get('fs_map')

    def get_subtrees(self, rank: str):
        log.debug('get_subtrees')
        cmd_dict = {'prefix': 'get subtrees'}

        r, outb, outs  = self.mgr.tell_command('mds', rank, cmd_dict)
        if r != 0:
            log.error(f"Invalid command dictionary: {cmd_dict} err {outs}")

        try:
            return json.loads(outb)
        except json.JSONDecodeError as e:
            raise MDSPartException(-errno.EINVAL, 'invalid json foromat for subtree')

    def _make_auth_tree_map(self, fs_name):
        log.debug('_make_auth_tree_map')
        auth_subtree_map = {}
        self.fs_map = self.mgr.get('fs_map')
        for rank in range(get_max_mds(self.fs_map, fs_name)):
            rank_subtree_info = self.get_subtrees(str(rank))
            for subtree in rank_subtree_info:
                if subtree['is_auth'] == True and subtree['auth_first'] == rank:
                    if len(subtree['dir']['path']) == 0:
                        dir_path = '/'
                    else:
                        dir_path = subtree['dir']['path']
                    log.debug(f'make_auth_tree: subtree {subtree}: auth {rank}')
                    auth_subtree_map[dir_path] = rank
        log.debug(f'auth_subtree_map:\n {auth_subtree_map}')
        return auth_subtree_map

    def _find_parent_auth_rank(self, auth_subtree_map, subtree_path):
        tokens = subtree_path.split('/')
        for idx in reversed(list(range(1, len(tokens) + 1))):
            subtree_path = '/'.join(tokens[0:idx])
            if len(subtree_path) == 0:
                subtree_path = '/'

            rank = auth_subtree_map.get(subtree_path, -1)
            if rank >= 0:
                log.debug(f'find {subtree_path} parent auth {rank}')
                return rank
        assert False

    def get_subtree_auth_map(self, fs_name, subtree_path_list):
        log.debug(f"get_subtree_auth_map: {fs_name}, {subtree_path_list}")
        auth_subtree_map = self._make_auth_tree_map(fs_name)
        my_subtree_auth_map = {}
        for subtree_path in subtree_path_list:
            rank = self._find_parent_auth_rank(auth_subtree_map, subtree_path)
            my_subtree_auth_map[subtree_path] = { 'rank': rank }

        log.debug(f'my_subtree_auth_map {my_subtree_auth_map}')
        return my_subtree_auth_map
