set(libradosstriper_srcs
  libradosstriper.cc
  RadosStriperImpl.cc
  MultiAioCompletionImpl.cc)
add_library(libradosstriper STATIC ${libradosstriper_srcs})
target_link_libraries(libradosstriper librados cls_lock_client osdc common pthread ${CRYPTO_LIBS} ${EXTRALIBS}) 
set_target_properties(libradosstriper PROPERTIES OUPUT_NAME libradosstriper VERSION "1.0.0" SOVERSION "1")
