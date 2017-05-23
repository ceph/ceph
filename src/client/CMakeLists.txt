set(libclient_srcs
  Client.cc
  Dentry.cc
  Fh.cc
  Inode.cc
  MetaRequest.cc
  ClientSnapRealm.cc
  MetaSession.cc
  Trace.cc
  posix_acl.cc)

if (HAVE_LIBRICHACL)
  list(APPEND libclient_srcs rich_acl.cc)
endif(HAVE_LIBRICHACL)

add_library(client STATIC ${libclient_srcs})
target_link_libraries(client osdc ${RICHACL_LIBS})
