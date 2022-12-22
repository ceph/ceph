set(lsvd_srcs
      liblsvd.cc  
      config.cc
      file_backend.cc
      io.cc
      lsvd_debug.cc
      mkcache.cc
      nvme.cc
      objects.cc
      rados_backend.cc
      read_cache.cc
      translate.cc
      write_cache.cc)
 
set(lsvd_libs
     librados
     pthread
     rt
     aio
     uuid
     z)

add_library(liblsvd SHARED
  ${lsvd_srcs})
target_link_libraries(liblsvd PRIVATE ${lsvd_libs}) 

if(ENABLE_SHARED)
  set_target_properties(liblsvd PROPERTIES
    OUTPUT_NAME lsvd
    VERSION 0.0.1
    SOVERSION 0
    SONAME rbd
    CXX_VISIBILITY_PRESET hidden
    VISIBILITY_INLINES_HIDDEN ON)
endif(ENABLE_SHARED)
install(TARGETS liblsvd DESTINATION ${CMAKE_INSTALL_LIBDIR})

add_executable(lsvd_imgtool
    lsvd_imgtool.cc
    ${lsvd_srcs})
target_link_libraries(lsvd_imgtool ${lsvd_libs}) 

add_executable(lsvd_rnd_test
    lsvd_rnd_test.cc
    ${lsvd_srcs})  
target_link_libraries(lsvd_rnd_test ${lsvd_libs}) 

add_executable(lsvd_crash_test
    lsvd_crash_test.cc
    ${lsvd_srcs}) 

target_link_libraries(lsvd_crash_test ${lsvd_libs}) 
