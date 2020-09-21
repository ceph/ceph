/*
 * This is an example RADOS object class built using only the Ceph SDK interface.
 */
#include "include/rados/objclass.h"

CLS_VER(1,0)
CLS_NAME(sdk)

cls_handle_t h_class;
cls_method_handle_t h_test_coverage_write;
cls_method_handle_t h_test_coverage_replay;

/**
 * test_coverage_write - a "write" method that creates an object
 *
 * This method modifies the object by making multiple write calls (write,
 * setxattr and set_val).
 */
static int test_coverage_write(cls_method_context_t hctx, ceph::buffer::list *in, ceph::buffer::list *out)
{
  // create the object
  int ret = cls_cxx_create(hctx, false);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: %s(): cls_cxx_create returned %d", __func__, ret);	  
    return ret;
  }

  uint64_t size;
  // get the size of the object
  ret = cls_cxx_stat(hctx, &size, NULL);
  if (ret < 0)
    return ret;
  
  std::string c = "test";
  ceph::buffer::list bl;
  bl.append(c);

  // write to the object
  ret = cls_cxx_write(hctx, 0, bl.length(), &bl);
  if (ret < 0)
    return ret;
  
  uint64_t new_size;
  // get the new size of the object
  ret = cls_cxx_stat(hctx, &new_size, NULL);
  if (ret < 0)
    return ret;

  // make some change to the xattr
  ret = cls_cxx_setxattr(hctx, "foo", &bl);
  if (ret < 0)
    return ret;

  // make some change to the omap
  ret = cls_cxx_map_set_val(hctx, "foo", &bl);
  if (ret < 0)
    return ret;

  return 0;
}

/**
 * test_coverage_replay - a "read" method to retrieve previously written data
 *
 * This method reads the object by making multiple read calls (read, getxattr 
 * and get_val). It also removes the object after reading.
 */

static int test_coverage_replay(cls_method_context_t hctx, ceph::buffer::list *in, ceph::buffer::list *out)
{
  CLS_LOG(0, "reading already written object");
  uint64_t size;
  // get the size of the object
  int ret = cls_cxx_stat(hctx, &size, NULL);
  if (ret < 0)
    return ret;
  
  ceph::buffer::list bl;
  // read the object entry
  ret = cls_cxx_read(hctx, 0, size, &bl);
  if (ret < 0)
    return ret;

  // if the size is incorrect 
  if (bl.length() != size)
    return -EIO;
  
  bl.clear();

  // read xattr entry
  ret = cls_cxx_getxattr(hctx, "foo", &bl);
  if (ret < 0)
    return ret;

  // if the size is incorrect
  if (bl.length() != size)
    return -EIO;
  
  bl.clear();

  // read omap entry
  ret = cls_cxx_map_get_val(hctx, "foo", &bl);
  if (ret < 0)
    return ret;

  // if the size is incorrect
  if (bl.length() != size)
    return -EIO;

  // remove the object
  ret = cls_cxx_remove(hctx);
  if (ret < 0)
    return ret;
  
  return 0;
}

CLS_INIT(sdk)
{
  CLS_LOG(0, "loading cls_sdk");

  cls_register("sdk", &h_class);

  cls_register_cxx_method(h_class, "test_coverage_write",
      CLS_METHOD_RD|CLS_METHOD_WR,
      test_coverage_write, &h_test_coverage_write);
  
  cls_register_cxx_method(h_class, "test_coverage_replay",
      CLS_METHOD_RD|CLS_METHOD_WR,
      test_coverage_replay, &h_test_coverage_replay);
}
