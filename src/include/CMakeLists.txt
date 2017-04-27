install(FILES rados/librados.h
  rados/rados_types.h
  rados/rados_types.hpp
  rados/librados.hpp
  buffer.h
  buffer_fwd.h
  inline_memory.h
  memory.h
  page.h
  crc32c.h
  DESTINATION include/rados)
install(FILES
  radosstriper/libradosstriper.h
  radosstriper/libradosstriper.hpp
  DESTINATION include/radosstriper)

if(WITH_RBD)
  install(FILES
    rbd/features.h
    rbd/librbd.h
    rbd/librbd.hpp
    DESTINATION include/rbd)
endif()

if(WITH_RADOSGW)
  install(FILES
    rados/librgw.h
    rados/rgw_file.h
  DESTINATION include/rados)
endif()
