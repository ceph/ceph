

add_executable(replayer ops_replayer.cc)

target_link_libraries(replayer
    PRIVATE
    fmt
    ceph-common
    librados
    Boost::program_options
)
