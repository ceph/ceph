add_library(lrem_mod_db STATIC
  mod/db/LRemDBStore.cc
  mod/db/LRemDBCluster.cc
  mod/db/LRemDBIoCtxImpl.cc
  mod/db/LRemDBRadosClient.cc)
target_include_directories(lrem_mod_db PUBLIC "${CMAKE_SOURCE_DIR}/src/libradosstub")
 
target_link_libraries(lrem_mod_db PRIVATE
  SQLiteCpp
  sqlite3)
 
add_library(lrem_mod_mem STATIC
  mod/mem/LRemMemCluster.cc
  mod/mem/LRemMemIoCtxImpl.cc
  mod/mem/LRemMemRadosClient.cc)
target_include_directories(lrem_mod_mem PUBLIC "${CMAKE_SOURCE_DIR}/src/libradosstub")

add_library(librados_stub_impl STATIC
  LRemClassHandler.cc
  LRemIoCtxImpl.cc
  LRemRadosClient.cc
  LRemWatchNotify.cc)

add_library(librados_stub_impl_db STATIC
  LRemDB.cc)
target_include_directories(librados_stub_impl_db PUBLIC "${CMAKE_SOURCE_DIR}/src/libradosstub")

add_library(librados_stub_impl_mem STATIC
  LRemMem.cc)
target_include_directories(librados_stub_impl_mem PUBLIC "${CMAKE_SOURCE_DIR}/src/libradosstub")

set(librados_stub_srcs
  LibradosLRemStub.cc
  NeoradosLRemStub.cc
  NObjects.cc)

foreach(TARGET_NAME rados_sqlite rados_mem)
  set(TARGET_LIB "lib${TARGET_NAME}")
  add_library(${TARGET_LIB} 
    ${CEPH_SHARED}
    ${librados_stub_srcs})

  if(ENABLE_SHARED)
    set_target_properties(${TARGET_LIB} PROPERTIES
      OUTPUT_NAME ${TARGET_NAME}
      VERSION 2.0.0
      SOVERSION 2
      VISIBILITY_INLINES_HIDDEN ON)
    if(HAVE_LINK_EXCLUDE_LIBS AND NOT WIN32)
      set_property(TARGET ${TARGET_LIB} APPEND_STRING PROPERTY
        LINK_FLAGS " -Wl,--exclude-libs,ALL")
    endif()
    if(HAVE_LINK_VERSION_SCRIPT AND NOT WIN32)
      set_property(TARGET ${TARGET_LIB} APPEND_STRING PROPERTY
        LINK_FLAGS " -Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/librados_stub.map")
    endif()
    if(WITH_STATIC_LIBSTDCXX)
      set_property(TARGET ${TARGET_LIB} APPEND_STRING PROPERTY
        LINK_FLAGS " -static-libstdc++ -static-libgcc")
    endif()
  endif()
endforeach(TARGET_NAME rados_sqlite rados_mem)

target_link_libraries(librados_sqlite PRIVATE
  librados_stub_impl
  librados_stub_impl_db
  lrem_mod_db
  ${BLKID_LIBRARIES} ${CRYPTO_LIBS} ${EXTRALIBS} ${GSSAPI_LIBRARIES})
install(TARGETS librados_sqlite DESTINATION ${CMAKE_INSTALL_LIBDIR})

target_link_libraries(librados_mem PRIVATE
  librados_stub_impl
  librados_stub_impl_mem
  lrem_mod_mem
  ${BLKID_LIBRARIES} ${CRYPTO_LIBS} ${EXTRALIBS} ${GSSAPI_LIBRARIES})
install(TARGETS librados_mem DESTINATION ${CMAKE_INSTALL_LIBDIR})
