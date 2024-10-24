

add_executable(replayer ops_replayer.cc)

target_link_libraries(replayer
    PRIVATE
    fmt
    librados
    pthread
    Boost::program_options
)
