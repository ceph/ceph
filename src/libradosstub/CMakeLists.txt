add_library(librados_stub_impl STATIC
  TestClassHandler.cc
  TestIoCtxImpl.cc
  TestMemCluster.cc
  TestMemIoCtxImpl.cc
  TestMemRadosClient.cc
  TestRadosClient.cc
  TestWatchNotify.cc)

# add_library(rados_stub STATIC ${librados_stub_srcs})
add_library(librados_stub ${CEPH_SHARED}
  LibradosTestStub.cc
  NeoradosTestStub.cc
  NObjects.cc)

if(ENABLE_SHARED)
  set_target_properties(librados_stub PROPERTIES
    OUTPUT_NAME rados_stub
    VERSION 2.0.0
    SOVERSION 2
    VISIBILITY_INLINES_HIDDEN ON)
  if(HAVE_LINK_EXCLUDE_LIBS AND NOT WIN32)
    set_property(TARGET librados_stub APPEND_STRING PROPERTY
      LINK_FLAGS " -Wl,--exclude-libs,ALL")
  endif()
  if(HAVE_LINK_VERSION_SCRIPT AND NOT WIN32)
    set_property(TARGET librados_stub APPEND_STRING PROPERTY
      LINK_FLAGS " -Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/librados_stub.map")
  endif()
  if(WITH_STATIC_LIBSTDCXX)
    set_property(TARGET librados_stub APPEND_STRING PROPERTY
      LINK_FLAGS " -static-libstdc++ -static-libgcc")
  endif()
endif()
target_link_libraries(librados_stub PRIVATE
  librados_stub_impl
  ${BLKID_LIBRARIES} ${CRYPTO_LIBS} ${EXTRALIBS} ${GSSAPI_LIBRARIES})
install(TARGETS librados_stub DESTINATION ${CMAKE_INSTALL_LIBDIR})
