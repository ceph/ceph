# scripts
add_ceph_test(check-generated.sh ${CMAKE_CURRENT_SOURCE_DIR}/check-generated.sh
	      LABELS "long" COST 619.0)
add_ceph_test(readable.sh ${CMAKE_CURRENT_SOURCE_DIR}/readable.sh
	      LABELS "long" COST 813.0)
