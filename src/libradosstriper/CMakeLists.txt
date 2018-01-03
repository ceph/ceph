set(libradosstriper_srcs
  libradosstriper.cc
  RadosStriperImpl.cc
  MultiAioCompletionImpl.cc)
add_library(radosstriper ${CEPH_SHARED}
  ${libradosstriper_srcs}
  $<TARGET_OBJECTS:librados_objs>)
target_link_libraries(radosstriper
  PRIVATE librados cls_lock_client osdc ceph-common pthread ${CRYPTO_LIBS} ${EXTRALIBS})
if(WITH_STATIC_LIBSTDCXX)
  target_link_libraries(radosstriper
    INTERFACE "-static-libstdc++ -static-libgcc")
endif()
set_target_properties(radosstriper PROPERTIES
  OUPUT_NAME radosstriper
  VERSION 1.0.0
  SOVERSION 1)

install(TARGETS radosstriper DESTINATION ${CMAKE_INSTALL_LIBDIR})
