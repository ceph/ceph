add_library(io_uring STATIC Ring.cxx Queue.cxx Operation.cxx)

pkg_check_modules(LIBURING REQUIRED liburing)
target_link_libraries(io_uring ${LIBURING_LIBRARIES})
target_include_directories(io_uring PUBLIC ${LIBURING_INCLUDE_DIRS})
target_compile_options(io_uring PUBLIC ${LIBURING_CFLAGS_OTHER})
