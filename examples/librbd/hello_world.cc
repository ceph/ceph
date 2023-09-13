// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */

// install the librados-dev and librbd package to get this
#include <rados/librados.hpp>
#include <rbd/librbd.hpp>
#include <iostream>
#include <string>
#include <sstream>

int main(int argc, const char **argv)
{
  int ret = 0;

  // we will use all of these below
  const char *pool_name = "hello_world_pool";
  std::string hello("hello world!");
  std::string object_name("hello_object");
  librados::IoCtx io_ctx;

  // first, we create a Rados object and initialize it
  librados::Rados rados;
  {
    ret = rados.init("admin"); // just use the client.admin keyring
    if (ret < 0) { // let's handle any error that might have come back
      std::cerr << "couldn't initialize rados! error " << ret << std::endl;
      ret = EXIT_FAILURE;
      goto out;
    } else {
      std::cout << "we just set up a rados cluster object" << std::endl;
    }
  }

  /*
   * Now we need to get the rados object its config info. It can
   * parse argv for us to find the id, monitors, etc, so let's just
   * use that.
   */
  {
    ret = rados.conf_parse_argv(argc, argv);
    if (ret < 0) {
      // This really can't happen, but we need to check to be a good citizen.
      std::cerr << "failed to parse config options! error " << ret << std::endl;
      ret = EXIT_FAILURE;
      goto out;
    } else {
      std::cout << "we just parsed our config options" << std::endl;
      // We also want to apply the config file if the user specified
      // one, and conf_parse_argv won't do that for us.
      for (int i = 0; i < argc; ++i) {
	if ((strcmp(argv[i], "-c") == 0) || (strcmp(argv[i], "--conf") == 0)) {
	  ret = rados.conf_read_file(argv[i+1]);
	  if (ret < 0) {
	    // This could fail if the config file is malformed, but it'd be hard.
	    std::cerr << "failed to parse config file " << argv[i+1]
	              << "! error" << ret << std::endl;
	    ret = EXIT_FAILURE;
	    goto out;
	  }
	  break;
	}
      }
    }
  }

  /*
   * next, we actually connect to the cluster
   */
  {
    ret = rados.connect();
    if (ret < 0) {
      std::cerr << "couldn't connect to cluster! error " << ret << std::endl;
      ret = EXIT_FAILURE;
      goto out;
    } else {
      std::cout << "we just connected to the rados cluster" << std::endl;
    }
  }

  /*
   * let's create our own pool instead of scribbling over real data.
   * Note that this command creates pools with default PG counts specified
   * by the monitors, which may not be appropriate for real use -- it's fine
   * for testing, though.
   */
  {
    ret = rados.pool_create(pool_name);
    if (ret < 0) {
      std::cerr << "couldn't create pool! error " << ret << std::endl;
      return EXIT_FAILURE;
    } else {
      std::cout << "we just created a new pool named " << pool_name << std::endl;
    }
  }

  /*
   * create an "IoCtx" which is used to do IO to a pool
   */
  {
    ret = rados.ioctx_create(pool_name, io_ctx);
    if (ret < 0) {
      std::cerr << "couldn't set up ioctx! error " << ret << std::endl;
      ret = EXIT_FAILURE;
      goto out;
    } else {
      std::cout << "we just created an ioctx for our pool" << std::endl;
    }
  }

  /*
   * create an rbd image and write data to it
   */
  {
    std::string name = "librbd_test";
    uint64_t size = 2 << 20;
    int order = 0;
    librbd::RBD rbd;
    librbd::Image image;

    ret = rbd.create(io_ctx, name.c_str(), size, &order);
    if (ret < 0) {
      std::cerr << "couldn't create an rbd image! error " << ret << std::endl;
      ret = EXIT_FAILURE;
      goto out;
    } else {
      std::cout << "we just created an rbd image" << std::endl;
    }

    ret = rbd.open(io_ctx, image, name.c_str(), NULL);
    if (ret < 0) {
      std::cerr << "couldn't open the rbd image! error " << ret << std::endl;
      ret = EXIT_FAILURE;
      goto out;
    } else {
      std::cout << "we just opened the rbd image" << std::endl;
    }

    int TEST_IO_SIZE = 512;
    char test_data[TEST_IO_SIZE + 1];
    int i;

    for (i = 0; i < TEST_IO_SIZE; ++i) {
      test_data[i] = (char) (rand() % (126 - 33) + 33);
    }
    test_data[TEST_IO_SIZE] = '\0';

    size_t len = strlen(test_data);
    ceph::bufferlist bl;
    bl.append(test_data, len);

    ret = image.write(0, len, bl);
    if (ret < 0) {
      std::cerr << "couldn't write to the rbd image! error " << ret << std::endl;
      ret = EXIT_FAILURE;
      goto out;
    } else {
      std::cout << "we just wrote data to our rbd image " << std::endl;
    }

    /*
     * let's read the image and compare it to the data we wrote
     */
    ceph::bufferlist bl_r;
    int read;
    read = image.read(0, TEST_IO_SIZE, bl_r);
    if (read < 0) {
      std::cerr << "we couldn't read data from the image! error" << std::endl;
      ret = EXIT_FAILURE;
      goto out;
    }
  
    std::string bl_res(bl_r.c_str(), read);
  
    int res = memcmp(bl_res.c_str(), test_data, TEST_IO_SIZE);
    if (res != 0) {
      std::cerr << "what we read didn't match expected! error" << std::endl;
    } else {
      std::cout << "we read our data on the image successfully" << std::endl;
    }

    image.close();

    /*
     *let's now delete the image
     */
    ret = rbd.remove(io_ctx, name.c_str());
    if (ret < 0) {
      std::cerr << "failed to delete rbd image! error " << ret << std::endl;
      ret = EXIT_FAILURE;
      goto out;
    } else {
      std::cout << "we just deleted our rbd image " << std::endl;
    }
  }

  ret = EXIT_SUCCESS;
  out:
  /*
   * And now we're done, so let's remove our pool and then
   * shut down the connection gracefully.
   */
  int delete_ret = rados.pool_delete(pool_name);
  if (delete_ret < 0) {
    // be careful not to
    std::cerr << "We failed to delete our test pool!" << std::endl;
    ret = EXIT_FAILURE;
  }

  rados.shutdown();

  return ret;
}
