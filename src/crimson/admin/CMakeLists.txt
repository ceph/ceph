add_library(crimson-admin STATIC
  admin_socket.cc
  osd_admin.cc
  pg_commands.cc)
target_compile_options(crimson-admin PRIVATE -fno-omit-frame-pointer)
target_link_libraries(crimson-admin
  legacy-option-headers
  crimson::cflags
  Boost::MPL)
