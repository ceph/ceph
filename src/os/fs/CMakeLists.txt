set(fs_srcs
  FS.cc)

if(HAVE_LIBXFS)
  list_(APPEND fs_srcs
    XFS.cc)
endif()
