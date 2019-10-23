#!/usr/bin/env nosetests

from unittest import TestCase

from runpy import run_path

cd = run_path("../ceph-daemon", run_name='nosetests')

class TestCephDaemon(TestCase):
    def test_pathify(self):
        self.assertEqual(cd['pathify']('a'), './a')
        self.assertEqual(cd['pathify']('/a/b'), '/a/b')

if __name__ == '__main__':
    unittest.main()
