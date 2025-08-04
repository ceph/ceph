set(dmc_srcs dmclock_util.cc ../support/src/run_every.cc)

add_library(dmclock STATIC ${dmc_srcs})
add_library(dmclock::dmclock ALIAS dmclock)

target_compile_options(dmclock PRIVATE
  "-Wno-write-strings" "-Wall")
target_include_directories(dmclock PUBLIC
  $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>
  $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/support/src>)

target_link_libraries(dmclock
  PUBLIC Boost::boost
  PRIVATE Threads::Threads)

if (WITH_CRIMSON)
  add_library(dmclock_crimson STATIC ${dmc_srcs})
  add_library(dmclock::crimson ALIAS dmclock_crimson)
  target_compile_definitions(dmclock_crimson PRIVATE WITH_CRIMSON)
  target_compile_options(dmclock_crimson PRIVATE -Wno-write-strings -Wall)

  target_include_directories(dmclock_crimson PRIVATE seastar)
  target_link_libraries(dmclock_crimson PRIVATE seastar)
endif()
