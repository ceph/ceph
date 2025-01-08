set(osdc_files
  Filer.cc
  ObjectCacher.cc
  error_code.cc)
# Objecter.cc and Striper.cc are part of libcommon
add_library(osdc STATIC ${osdc_files})
target_link_libraries(osdc ceph-common)
if(WITH_LTTNG)
  # objecter.h includes tracing/osdc.h
  add_dependencies(osdc osdc-tp)
endif()
if(WITH_EVENTTRACE)
  add_dependencies(osdc eventtrace_tp)
endif()
