from tasks.cephfs.cephfs_test_case import CephFSTestCase

class TestMetaInjection(CephFSTestCase):
    def test_meta_injection(self):
        conf_ori = self.fs.mds_asok(['config', 'show'])
        self.fs.mds_asok(['config', 'set', 'mds_log_max_segments', '1'])
        self.mount_a.run_shell(["mkdir", "metadir"])
        self.mount_a.run_shell(["touch", "metadir/metafile1"])
        self.mount_a.run_shell(["touch", "metadir/metafile2"])
        self.fs.mds_asok(['flush', 'journal'])
        dirino = self.mount_a.path_to_ino("metadir") 
        ino = self.mount_a.path_to_ino("metadir/metafile1")
        
        # export meta of ino
        self.fs.meta_tool(['showm', '-i', str(ino), '-o', '/tmp/meta_out'], 0, True)
        out = self.mount_a.run_shell(['grep', str(ino),'/tmp/meta_out']).stdout.getvalue().strip()
        
        # check the metadata of ino
        self.assertNotEqual(out.find(u'"ino":'+ str(ino)), -1)
        
        # amend info of ino
        self.fs.get_meta_of_fs_file(dirino, "metafile1", "/tmp/meta_obj")
        self.fs.meta_tool(['amend', '-i', str(ino), '--in', '/tmp/meta_out', '--yes-i-really-really-mean-it'], 0, True)
        self.fs.get_meta_of_fs_file(dirino, "metafile1", "/tmp/meta_obj_chg")
        
        # checkout meta_out after import it
        ori_mds5 = self.mount_a.run_shell(["md5sum", "/tmp/meta_obj"]).stdout.getvalue().strip().split()
        chg_mds5 = self.mount_a.run_shell(["md5sum", "/tmp/meta_obj_chg"]).stdout.getvalue().strip().split()
        print(ori_mds5," ==> ", chg_mds5)
        self.assertEqual(len(ori_mds5), 2)
        self.assertEqual(len(chg_mds5), 2)
        self.assertEqual(ori_mds5[0], chg_mds5[0])

        self.mount_a.run_shell(["rm", "metadir", "-rf"])
        self.mount_a.run_shell(["rm", "/tmp/meta_obj"])
        self.mount_a.run_shell(["rm", "/tmp/meta_obj_chg"])
        # restore config of mds_log_max_segments
        self.fs.mds_asok(['config', 'set', 'mds_log_max_segments', conf_ori["mds_log_max_segments"]])
