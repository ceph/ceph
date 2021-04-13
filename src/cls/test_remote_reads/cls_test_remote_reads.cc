/*
 * This is an example RADOS object class that shows how to use remote reads.
 */

#include "common/ceph_json.h"
#include "objclass/objclass.h"

CLS_VER(1,0)
CLS_NAME(test_remote_reads)

cls_handle_t h_class;
cls_method_handle_t h_test_read;
cls_method_handle_t h_test_gather;

/**
 * read data
 */
static int test_read(cls_method_context_t hctx, bufferlist *in, bufferlist *out) {
  int r = cls_cxx_read(hctx, 0, 0, out);
  if (r < 0) {
    CLS_ERR("%s: error reading data", __PRETTY_FUNCTION__);
    return r;
  }
  return 0;
}

/**
 * gather data from other objects using remote reads
 */
static int test_gather(cls_method_context_t hctx, bufferlist *in, bufferlist *out) {
  std::map<std::string, bufferlist> src_obj_buffs;
  int r = cls_cxx_get_gathered_data(hctx, &src_obj_buffs);
  if (src_obj_buffs.empty()) {
    // start remote reads
    JSONParser parser;
    bool b = parser.parse(in->c_str(), in->length());
    if (!b) {
      CLS_ERR("%s: failed to parse json", __PRETTY_FUNCTION__);
      return -EBADMSG;
    }
    auto *o_cls = parser.find_obj("cls");
    ceph_assert(o_cls);
    std::string cls = o_cls->get_data_val().str;

    auto *o_method = parser.find_obj("method");
    ceph_assert(o_method);
    std::string method = o_method->get_data_val().str;

    auto *o_pool = parser.find_obj("pool");
    ceph_assert(o_pool);
    std::string pool = o_pool->get_data_val().str;

    auto *o_src_objects = parser.find_obj("src_objects");
    ceph_assert(o_src_objects);
    auto src_objects_v = o_src_objects->get_array_elements();
    std::set<std::string> src_objects;
    for (auto it = src_objects_v.begin(); it != src_objects_v.end(); it++) {
      std::string oid_without_double_quotes = it->substr(1, it->size()-2);
      src_objects.insert(oid_without_double_quotes);
    }
    r = cls_cxx_gather(hctx, src_objects, pool, cls.c_str(), method.c_str(), *in);
  } else {
    // write data gathered using remote reads
    int offset = 0;
    for (std::map<std::string, bufferlist>::iterator it = src_obj_buffs.begin(); it != src_obj_buffs.end(); it++) {
      bufferlist bl= it->second;
      r = cls_cxx_write(hctx, offset, bl.length(), &bl);
      offset += bl.length();
    }
  }
  return r;
}

CLS_INIT(test_remote_reads)
{
  CLS_LOG(0, "loading cls_test_remote_reads");

  cls_register("test_remote_reads", &h_class);
  
  cls_register_cxx_method(h_class, "test_read",
			  CLS_METHOD_RD,
			  test_read, &h_test_read);

  cls_register_cxx_method(h_class, "test_gather",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  test_gather, &h_test_gather);
}
