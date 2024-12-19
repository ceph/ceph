

add_executable(replayer ops_replayer.cc ops_parser.cc)

target_link_libraries(replayer
    PRIVATE
    fmt
    librados
    Boost::program_options
)


add_executable(fragment_check ops_frag_reuse_check.cc ops_parser.cc)

target_link_libraries(fragment_check
    PRIVATE
    fmt
    librados
    Boost::program_options
)
