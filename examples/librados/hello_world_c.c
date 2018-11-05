// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 * Copyright 2013 Inktank
 */

// install the librados-dev package to get this
#include <rados/librados.h>
#include <stdio.h>
#include <stdlib.h>

int main(int argc, const char **argv)
{
  int ret = 0;

  // we will use all of these below
  const char *pool_name = "hello_world_pool";
  const char* hello = "hello world!";
  const char* object_name = "hello_object";
  rados_ioctx_t io_ctx = NULL;
  int pool_created = 0;

  // first, we create a Rados object and initialize it
  rados_t rados = NULL;
  {
    ret = rados_create(&rados, "admin"); // just use the client.admin keyring
    if (ret < 0) { // let's handle any error that might have come back
      printf("couldn't initialize rados! error %d\n", ret);
      ret = EXIT_FAILURE;
      goto out;
    }
    printf("we just set up a rados cluster object\n");
  }

  /*
   * Now we need to get the rados object its config info. It can
   * parse argv for us to find the id, monitors, etc, so let's just
   * use that.
   */
  {
    ret = rados_conf_parse_argv(rados, argc, argv);
    if (ret < 0) {
      // This really can't happen, but we need to check to be a good citizen.
      printf("failed to parse config options! error %d\n", ret);
      ret = EXIT_FAILURE;
      goto out;
    }

    printf("we just parsed our config options\n");
    // We also want to apply the config file if the user specified
    // one, and conf_parse_argv won't do that for us.
    int i;
    for (i = 0; i < argc; ++i) {
      if ((strcmp(argv[i], "-c") == 0) || (strcmp(argv[i], "--conf") == 0)) {
        ret = rados_conf_read_file(rados, argv[i+1]);
        if (ret < 0) {
          // This could fail if the config file is malformed, but it'd be hard.
          printf("failed to parse config file %s! error %d\n", argv[i+1], ret);
          ret = EXIT_FAILURE;
          goto out;
        }
        break;
      }
    }
  }

  /*
   * next, we actually connect to the cluster
   */
  {
    ret = rados_connect(rados);
    if (ret < 0) {
      printf("couldn't connect to cluster! error %d\n", ret);
      ret = EXIT_FAILURE;
      goto out;
    }
    printf("we just connected to the rados cluster\n");
  }

  /*
   * let's create our own pool instead of scribbling over real data.
   * Note that this command creates pools with default PG counts specified
   * by the monitors, which may not be appropriate for real use -- it's fine
   * for testing, though.
   */
  {
    ret = rados_pool_create(rados, pool_name);
    if (ret < 0) {
      printf("couldn't create pool! error %d\n", ret);
      return EXIT_FAILURE;
    }
    printf("we just created a new pool named %s\n", pool_name);
    pool_created = 1;
  }

  /*
   * create an "IoCtx" which is used to do IO to a pool
   */
  {
    ret = rados_ioctx_create(rados, pool_name, &io_ctx);
    if (ret < 0) {
      printf("couldn't set up ioctx! error %d\n", ret);
      ret = EXIT_FAILURE;
      goto out;
    }
    printf("we just created an ioctx for our pool\n");
  }

  /*
   * now let's do some IO to the pool! We'll write "hello world!" to a
   * new object.
   */
  {
    /*
     * now that we have the data to write, let's send it to an object.
     * We'll use the synchronous interface for simplicity.
     */
    ret = rados_write_full(io_ctx, object_name, hello, strlen(hello));
    if (ret < 0) {
      printf("couldn't write object! error %d\n", ret);
      ret = EXIT_FAILURE;
      goto out;
    }
    printf("we just wrote new object %s, with contents '%s'\n", object_name, hello);
  }

  /*
   * now let's read that object back! Just for fun, we'll do it using
   * async IO instead of synchronous. (This would be more useful if we
   * wanted to send off multiple reads at once; see
   * http://docs.ceph.com/docs/master/rados/api/librados/#asychronous-io )
   */
  {
    int read_len = 4194304; // this is way more than we need
    char* read_buf = malloc(read_len + 1); // add one for the terminating 0 we'll add later
    if (!read_buf) {
      printf("couldn't allocate read buffer\n");
      ret = EXIT_FAILURE;
      goto out;
    }
    // allocate the completion from librados
    rados_completion_t read_completion;
    ret = rados_aio_create_completion(NULL, NULL, NULL, &read_completion);
    if (ret < 0) {
      printf("couldn't create completion! error %d\n", ret);
      ret = EXIT_FAILURE;
      free(read_buf);
      goto out;
    }
    printf("we just created a new completion\n");

    // send off the request.
    ret = rados_aio_read(io_ctx, object_name, read_completion, read_buf, read_len, 0);
    if (ret < 0) {
      printf("couldn't start read object! error %d\n", ret);
      ret = EXIT_FAILURE;
      free(read_buf);
      rados_aio_release(read_completion);
      goto out;
    }
    // wait for the request to complete, and check that it succeeded.
    rados_aio_wait_for_complete(read_completion);
    ret = rados_aio_get_return_value(read_completion);
    if (ret < 0) {
      printf("couldn't read object! error %d\n", ret);
      ret = EXIT_FAILURE;
      free(read_buf);
      rados_aio_release(read_completion);
      goto out;
    }
    read_buf[ret] = 0; // null-terminate the string
    printf("we read our object %s, and got back %d bytes with contents\n%s\n", object_name, ret, read_buf);

    free(read_buf);
    rados_aio_release(read_completion);
  }

  /*
   * We can also use xattrs that go alongside the object.
   */
  {
    const char* version = "1";
    ret = rados_setxattr(io_ctx, object_name, "version", version, strlen(version));
    if (ret < 0) {
      printf("failed to set xattr version entry! error %d\n", ret);
      ret = EXIT_FAILURE;
      goto out;
    }
    printf("we set the xattr 'version' on our object!\n");
  }

  /*
   * And if we want to be really cool, we can do multiple things in a single
   * atomic operation. For instance, we can update the contents of our object
   * and set the version at the same time.
   */
  {
    const char* content = "v2";
    rados_write_op_t write_op = rados_create_write_op();
    if (!write_op) {
      printf("failed to allocate write op\n");
      ret = EXIT_FAILURE;
      goto out;
    }
    rados_write_op_write_full(write_op, content, strlen(content));
    const char* version = "2";
    rados_write_op_setxattr(write_op, "version", version, strlen(version));
    ret = rados_write_op_operate(write_op, io_ctx, object_name, NULL, 0);
    if (ret < 0) {
      printf("failed to do compound write! error %d\n", ret);
      ret = EXIT_FAILURE;
      rados_release_write_op(write_op);
      goto out;
    }
    printf("we overwrote our object %s with contents\n%s\n", object_name, content);
    rados_release_write_op(write_op);
  }

  /*
   * And to be even cooler, we can make sure that the object looks the
   * way we expect before doing the write! Notice how this attempt fails
   * because the xattr differs.
   */
  {
    rados_write_op_t failed_write_op = rados_create_write_op();
    if (!failed_write_op) {
      printf("failed to allocate write op\n");
      ret = EXIT_FAILURE;
      goto out;
    }
    const char* content = "v2";
    const char* version = "2";
    const char* old_version = "1";
    rados_write_op_cmpxattr(failed_write_op, "version", LIBRADOS_CMPXATTR_OP_EQ, old_version, strlen(old_version));
    rados_write_op_write_full(failed_write_op, content, strlen(content));
    rados_write_op_setxattr(failed_write_op, "version", version, strlen(version));
    ret = rados_write_op_operate(failed_write_op, io_ctx, object_name, NULL, 0);
    if (ret < 0) {
      printf("we just failed a write because the xattr wasn't as specified\n");
    } else {
      printf("we succeeded on writing despite an xattr comparison mismatch!\n");
      ret = EXIT_FAILURE;
      rados_release_write_op(failed_write_op);
      goto out;
    }
    rados_release_write_op(failed_write_op);

    /*
     * Now let's do the update with the correct xattr values so it
     * actually goes through
     */
    content = "v3";
    old_version = "2";
    version = "3";
    rados_write_op_t update_op = rados_create_write_op();
    if (!failed_write_op) {
      printf("failed to allocate write op\n");
      ret = EXIT_FAILURE;
      goto out;
    }
    rados_write_op_cmpxattr(update_op, "version", LIBRADOS_CMPXATTR_OP_EQ, old_version, strlen(old_version));
    rados_write_op_write_full(update_op, content, strlen(content));
    rados_write_op_setxattr(update_op, "version", version, strlen(version));
    ret = rados_write_op_operate(update_op, io_ctx, object_name, NULL, 0);
    if (ret < 0) {
      printf("failed to do a compound write update! error %d\n", ret);
      ret = EXIT_FAILURE;
      rados_release_write_op(update_op);
      goto out;
    }
    printf("we overwrote our object %s following an xattr test with contents\n%s\n", object_name, content);
    rados_release_write_op(update_op);
  }

  ret = EXIT_SUCCESS;

 out:
  if (io_ctx) {
    rados_ioctx_destroy(io_ctx);
  }

  if (pool_created) {
    /*
     * And now we're done, so let's remove our pool and then
     * shut down the connection gracefully.
     */
    int delete_ret = rados_pool_delete(rados, pool_name);
    if (delete_ret < 0) {
      // be careful not to
      printf("We failed to delete our test pool!\n");
      ret = EXIT_FAILURE;
    }
  }

  rados_shutdown(rados);

  return ret;
}
