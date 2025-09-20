add_library(object_io_exerciser STATIC
  DataGenerator.cc
  IoOp.cc
  IoSequence.cc
  Model.cc
  ObjectModel.cc
  RadosIo.cc
  EcIoSequence.cc
)

target_link_libraries(object_io_exerciser
  librados
  global
  json_structures
)

# libstdc++ uses TBB to implement <execution> if it is available,
# which means that if we're going to use <execution>, we need to link
# against TBB if it's available.
#
# It happens to work by accident at present because the optimizer
# optimizes out our one reference, for now, but this breaks compiling
# with `-O0` and will break anyway once we try to do more.
find_package(TBB QUIET)
if(TBB_FOUND)
  message(STATUS "Linking to TBB for implementations of <execution>.")
  target_link_libraries(object_io_exerciser TBB::tbb)
endif(TBB_FOUND)
