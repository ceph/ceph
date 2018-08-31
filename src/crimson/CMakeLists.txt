set(crimson_net_srcs
  net/Dispatcher.cc
  net/Errors.cc
  net/SocketConnection.cc
  net/SocketMessenger.cc)
set(crimson_thread_srcs
  thread/ThreadPool.cc
  thread/Throttle.cc)
set(crimson_common_srcs
  common/config_proxy.cc
  common/assert.cc
  common/log.cc)
add_library(crimson STATIC
  ${crimson_common_srcs}
  ${crimson_net_srcs}
  ${crimson_thread_srcs}
  ${CMAKE_SOURCE_DIR}/src/common/buffer_seastar.cc)
target_link_libraries(crimson Seastar::seastar ceph-common)
