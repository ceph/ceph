set(librados_test_stub_srcs
  LibradosTestStub.cc
  TestClassHandler.cc
  TestIoCtxImpl.cc
  TestMemIoCtxImpl.cc
  TestMemRadosClient.cc
  TestRadosClient.cc
  TestWatchNotify.cc)
add_library(rados_test_stub STATIC ${librados_test_stub_srcs})
  
