add_ceph_test(test_ceph_daemon.py
  ${Python3_EXECUTABLE} -m nose ${CMAKE_CURRENT_SOURCE_DIR}/test_ceph_daemon.py)
add_ceph_test(test_ceph_argparse.py
  ${Python3_EXECUTABLE} -m nose ${CMAKE_CURRENT_SOURCE_DIR}/test_ceph_argparse.py)
