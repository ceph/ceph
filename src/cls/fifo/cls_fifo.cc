// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/** \file
 *
 * This is an OSD class that implements methods for management
 * and use of fifo
 *
 */

#include <errno.h>

#include "objclass/objclass.h"

#include "cls/fifo/cls_fifo_ops.h"
#include "cls/fifo/cls_fifo_types.h"


using namespace rados::cls::fifo;


CLS_VER(1,0)
CLS_NAME(fifo)

struct cls_fifo_header {
  string id;
  struct {
    string name;
    string ns;
  } pool;
  string oid_prefix;

  uint64_t tail_obj_num{0};
  uint64_t head_obj_num{0};

  string head_tag;

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    encode(pool.name, bl);
    encode(pool.ns, bl);
    encode(oid_prefix, bl);
    encode(tail_obj_num, bl);
    encode(head_obj_num, bl);
    encode(head_tag, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    decode(pool.name, bl);
    decode(pool.ns, bl);
    decode(oid_prefix, bl);
    decode(tail_obj_num, bl);
    decode(head_obj_num, bl);
    decode(head_tag, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_fifo_header)

struct cls_fifo_data_obj_header {
  string tag;

  fifo_data_params_t params;

  uint64_t magic{0};

  uint64_t min_ofs{0};
  uint64_t max_ofs{0};
  uint64_t min_index{0};
  uint64_t max_index{0};

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(tag, bl);
    encode(params, bl);
    encode(magic, bl);
    encode(min_ofs, bl);
    encode(max_ofs, bl);
    encode(min_index, bl);
    encode(max_index, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(tag, bl);
    decode(params, bl);
    decode(magic, bl);
    decode(min_ofs, bl);
    decode(max_ofs, bl);
    decode(min_index, bl);
    decode(max_index, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_fifo_data_obj_header)

struct cls_fifo_entry_header_pre {
/* FIXME: le64_t */
  uint64_t magic{0};
  uint64_t header_size{0};
};

struct cls_fifo_entry_header {
  uint64_t index{0};
  uint64_t size{0};
  ceph::real_time mtime;

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(index, bl);
    encode(size, bl);
    encode(mtime, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(index, bl);
    decode(size, bl);
    decode(mtime, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_fifo_entry_header)


static string new_oid_prefix(string id, std::optional<string>& val)
{
  if (val) {
    return *val;
  }

#define PREFIX_RND_SIZE 12

  char buf[PREFIX_RND_SIZE + 1];
  buf[PREFIX_RND_SIZE] = 0;

  cls_gen_rand_base64(buf, sizeof(buf) - 1);

  char s[id.size() + 1 + sizeof(buf) + 16];
  snprintf(s, sizeof(s), "%s.%s", id.c_str(), buf);
  return s;
}

static int fifo_create_op(cls_method_context_t hctx,
                          bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "%s", __func__);

  cls_fifo_create_op op;
  try {
    auto iter = in->cbegin();
    decode(op, iter);
  } catch (const buffer::error &err) {
    CLS_ERR("ERROR: %s(): failed to decode request", __func__);
    return -EINVAL;
  }

  uint64_t size;

  int r = cls_cxx_stat2(hctx, &size, nullptr);
  if (r < 0 && r != -ENOENT) {
    CLS_ERR("ERROR: %s(): cls_cxx_stat2() on obj returned %d", __func__, r);
    return r;
  }
  if (op.exclusive && r == 0) {
    CLS_LOG(10, "%s(): exclusive create but queue already exists", __func__);
    return -EEXIST;
  }

  if (r == 0) {
    bufferlist bl;
    r = cls_cxx_read2(hctx, 0, size, &bl, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
    if (r < 0) {
      CLS_ERR("ERROR: %s(): cls_cxx_read2() on obj returned %d", __func__, r);
      return r;
    }

    cls_fifo_header header;
    try {
      auto iter = bl.cbegin();
      decode(header, iter);
    } catch (buffer::error& err) {
      CLS_ERR("ERROR: %s(): failed decoding header", __func__);
      return -EIO;
    }

    if (!(header.id == op.id &&
          header.pool.name == op.pool.name &&
          header.pool.ns == op.pool.ns &&
          (!op.oid_prefix ||
           header.oid_prefix == *op.oid_prefix))) {
      CLS_LOG(10, "%s(): failed to re-create existing queue with different params", __func__);
      return -EINVAL;
    }

    return 0; /* already exists */
  }
  cls_fifo_header header;
  
  header.id = op.id;
  header.oid_prefix = new_oid_prefix(op.id, op.oid_prefix);
  header.pool.name = op.pool.name;
  header.pool.ns = op.pool.ns;

  bufferlist bl;
  encode(header, bl);

  r = cls_cxx_write_full(hctx, &bl);
  if (r < 0) {
    CLS_LOG(10, "%s(): failed to write header: r=%d", __func__, r);
    return r;
  }

  return 0;
}

CLS_INIT(fifo)
{
  CLS_LOG(20, "Loaded fifo class!");

  cls_handle_t h_class;
  cls_method_handle_t h_fifo_create_op;

  cls_register("fifo", &h_class);
  cls_register_cxx_method(h_class, "fifo_create",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          fifo_create_op, &h_fifo_create_op);

  return;
}
