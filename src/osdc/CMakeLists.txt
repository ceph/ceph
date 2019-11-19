set(osdc_files
  Filer.cc
  ObjectCacher.cc
  Objecter.cc
  error_code.cc
  Striper.cc)
add_library(osdc STATIC ${osdc_files})
if(WITH_EVENTTRACE)
  add_dependencies(osdc eventtrace_tp)
endif()
