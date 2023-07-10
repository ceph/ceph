add_library(isal_md5 STATIC async_md5.cc)
target_link_libraries(isal_md5 PUBLIC ISAL::Crypto spawn)
target_include_directories(isal_md5 SYSTEM INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
