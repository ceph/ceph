from tasks.cephfs.cephfs_test_case import CephFSTestCase
import random
import os

class TestDumpTree(CephFSTestCase):
    def get_paths_to_ino(self):
        inos = {}
        p = self.mount_a.run_shell(["find", "./"])
        paths = p.stdout.getvalue().strip().split()
        for path in paths:
            inos[path] = self.mount_a.path_to_ino(path, False)

        return inos

    def populate(self):
        self.mount_a.run_shell(["git", "clone",
                                "https://github.com/ceph/ceph-qa-suite"])

    def test_basic(self):
        self.mount_a.run_shell(["mkdir", "parent"])
        self.mount_a.run_shell(["mkdir", "parent/child"])
        self.mount_a.run_shell(["touch", "parent/child/file"])
        self.mount_a.run_shell(["mkdir", "parent/child/grandchild"])
        self.mount_a.run_shell(["touch", "parent/child/grandchild/file"])

        inos = self.get_paths_to_ino()
        tree = self.fs.mds_asok(["dump", "tree", "/parent/child", "1"])

        target_inos = [inos["./parent/child"], inos["./parent/child/file"],
                       inos["./parent/child/grandchild"]]

        for ino in tree:
            del target_inos[target_inos.index(ino['ino'])] # don't catch!
            
        assert(len(target_inos) == 0)

    def test_random(self):
        random.seed(0)

        self.populate()
        inos = self.get_paths_to_ino()
        target = random.choice(inos.keys())

        if target != "./":
            target = os.path.dirname(target)

        subtree = [path for path in inos.keys() if path.startswith(target)]
        target_inos = [inos[path] for path in subtree]
        tree = self.fs.mds_asok(["dump", "tree", target[1:]])

        for ino in tree:
            del target_inos[target_inos.index(ino['ino'])] # don't catch!
            
        assert(len(target_inos) == 0)

        target_depth = target.count('/')
        maxdepth = max([path.count('/') for path in subtree]) - target_depth
        depth = random.randint(0, maxdepth)
        target_inos = [inos[path] for path in subtree \
                       if path.count('/') <= depth + target_depth]
        tree = self.fs.mds_asok(["dump", "tree", target[1:], str(depth)])

        for ino in tree:
            del target_inos[target_inos.index(ino['ino'])] # don't catch!
            
        assert(len(target_inos) == 0)
