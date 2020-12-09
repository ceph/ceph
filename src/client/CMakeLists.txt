set(libclient_srcs
  Client.cc
  Dentry.cc
  Fh.cc
  Inode.cc
  MetaRequest.cc
  ClientSnapRealm.cc
  MetaSession.cc
  Trace.cc
  posix_acl.cc
  Delegation.cc)
add_library(client STATIC ${libclient_srcs})
target_link_libraries(client osdc dmclock::dmclock)
target_include_directories(client PUBLIC "${CMAKE_SOURCE_DIR}/src/dmclock/support/src")
