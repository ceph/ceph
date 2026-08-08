// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <iostream>
#include <errno.h>
#include <string>

#include "cls/hello/cls_hello_ops.h"
#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/encoding.h"
#include "test/librados/test.h"
#include "test/librados/test_cxx.h"
#include "gtest/gtest.h"

using namespace librados;
using namespace cls::hello;

/**
 * Test cls method read/write policing for umbrella clients
 *
 * This test verifies that the OSD correctly enforces read/write
 * operation policing for cls methods when clients have the
 * CEPH_FEATURE_SERVER_UMBRELLA feature flag set.
 */
TEST(ClsMethodPolicing, BadReaderWithUmbrellaClient) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  bufferlist in, out;

  // Create the object first
  ASSERT_EQ(0, ioctx.write_full("myobject", in));

  // The bad_reader method is registered as WR (write) but tries to read
  // For umbrella clients, this should return -EIO as the method
  // attempts to read but is not marked with RD flag
  ObjectWriteOperation write_operation;
  int rval;
  write_operation.exec(method::bad_reader, in, &out, &rval);
  int result = ioctx.operate("myobject", &write_operation);

  // The operation should fail with -EIO because bad_reader tries to read
  // but is not marked with CLS_METHOD_RD flag
  ASSERT_EQ(-EIO, result);
  ASSERT_EQ(-EIO, rval);

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

/**
 * Test cls method write policing for umbrella clients
 *
 * This test verifies that methods marked as RD (read-only) that
 * attempt to write are properly caught and rejected.
 */
TEST(ClsMethodPolicing, BadWriterWithUmbrellaClient) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  bufferlist in, out;

  // Create the object first
  ASSERT_EQ(0, ioctx.write_full("myobject", in));

  // The bad_writer method is registered as RD (read) but tries to write
  // This should return -EIO
  int result = ioctx.exec("myobject", method::bad_writer, in, out);

  // The operation should fail with -EIO because bad_writer tries to write
  // but is not marked with CLS_METHOD_WR flag
  ASSERT_EQ(-EIO, result);

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

/**
 * Test that properly marked methods still work correctly
 *
 * This test ensures that the policing doesn't break correctly
 * implemented methods that are properly marked with RD/WR flags.
 */
TEST(ClsMethodPolicing, GoodUmbrellaClientSuccess) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  bufferlist in, out;

  // Test a read-only method (say_hello) - should work fine
  ASSERT_EQ(0, ioctx.write_full("myobject", in));
  ASSERT_EQ(0, ioctx.exec("myobject", method::say_hello, in, out));
  ASSERT_EQ(std::string("Hello, world!"), std::string(out.c_str(), out.length()));

  // Test a write method (record_hello) - should work fine
  out.clear();
  in.clear();
  ObjectWriteOperation write_operation;
  int rval;
  write_operation.exec(method::record_hello, in, &out, &rval);
  ASSERT_EQ(0, ioctx.operate("myobject2", &write_operation));
  ASSERT_EQ(0, rval);

  // Test a read-modify-write method (turn_it_to_11) - should work fine
  out.clear();
  in.clear();
  ObjectWriteOperation write_operation2;
  write_operation2.exec(method::turn_it_to_11, in, &out, &rval);
  ASSERT_EQ(0, ioctx.operate("myobject2", &write_operation2));
  ASSERT_EQ(0, rval);

  // Verify the result
  out.clear();
  ASSERT_EQ(0, ioctx.exec("myobject2", method::replay, in, out));
  ASSERT_EQ(std::string("HELLO, WORLD!"), std::string(out.c_str(), out.length()));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

/**
 * Test multiple operations in a single transaction
 *
 * This test verifies that policing works correctly when multiple
 * cls method calls are batched in a single operation.
 */
TEST(ClsMethodPolicing, MultipleOperationsInTransaction) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  bufferlist in, out;

  // Create object and do a valid operation
  ObjectWriteOperation write_operation;
  int rval1, rval2;
  bufferlist out1, out2;

  write_operation.exec(method::record_hello, in, &out1, &rval1);
  // Add a bad operation - this should cause the entire transaction to fail
  write_operation.exec(method::bad_reader, in, &out2, &rval2);

  int result = ioctx.operate("myobject", &write_operation);

  // The entire operation should fail because of the bad_reader
  ASSERT_EQ(-EIO, result);

  // Verify the object was not created (transaction was rolled back)
  bufferlist read_out;
  ASSERT_EQ(-ENOENT, ioctx.read("myobject", read_out, 100, 0));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}
/**
 * Test pre-execution policing with read operation containing WR method
 *
 * This test verifies that when multiple cls methods are batched in a READ
 * operation, and one of them is marked as WR (write), the pre-execution
 * check catches the mismatch and returns -EINVAL before execution.
 *
 * Uses the depricated C API (rados_read_op_exec) which lacks compile-time type
 * safety, allowing us to call WR methods in read operations to test server-side
 * policing.
 */
TEST(ClsMethodPolicing, MultipleOperationsWithABadMethod) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  bufferlist in;

  // Create the object first using C++ API
  ASSERT_EQ(0, ioctx.write_full("myobject", in));

  // Create a C API ioctx for the same pool to use with C API operations
  rados_t c_cluster;
  ASSERT_EQ("", connect_cluster(&c_cluster));
  rados_ioctx_t c_ioctx;
  ASSERT_EQ(0, rados_ioctx_create(c_cluster, pool_name.c_str(), &c_ioctx));

  // Use C API to create a READ operation with multiple cls method calls
  // The C API doesn't have compile-time type safety, so we can call WR methods
  rados_read_op_t read_op = rados_create_read_op();
  
  // First operation: say_hello (RD method) - should be fine in a read operation
  char *out1 = NULL;
  size_t out1_len = 0;
  int rval1 = 0;
  rados_read_op_exec(read_op, "hello", "say_hello", NULL, 0, &out1, &out1_len,
                     &rval1);

  // Second operation: bad_reader (WR method) - should fail pre-execution check
  // because it's marked as WR but we're in a READ operation
  char *out2 = NULL;
  size_t out2_len = 0;
  int rval2 = 0;
  rados_read_op_exec(read_op, "hello", "bad_reader", NULL, 0, &out2, &out2_len,
                     &rval2);

  // Execute the read operation
  int result = rados_read_op_operate(read_op, c_ioctx, "myobject", 0);

  ASSERT_EQ(-EINVAL, result);

  // Verify that no data was returned from either operation
  ASSERT_EQ(nullptr, out1);
  ASSERT_EQ(0u, out1_len);
  ASSERT_EQ(nullptr, out2);
  ASSERT_EQ(0u, out2_len);

  rados_release_read_op(read_op);
  rados_ioctx_destroy(c_ioctx);
  rados_shutdown(c_cluster);

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}
