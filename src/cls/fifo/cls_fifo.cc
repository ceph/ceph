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

static int write_header(cls_method_context_t hctx,
                        fifo_info_t& header)
{
  if (header.objv.instance.empty()) {
#define HEADER_INSTANCE_SIZE 16
  char buf[HEADER_INSTANCE_SIZE + 1];
  buf[HEADER_INSTANCE_SIZE] = 0;
  cls_gen_rand_base64(buf, sizeof(buf) - 1);

    header.objv.instance = buf;
  }
  ++header.objv.ver;
  bufferlist bl;
  encode(header, bl);
  return cls_cxx_write_full(hctx, &bl);
}

const char *part_header_xattr_name = "fifo.part.header";

static int read_part_header(cls_method_context_t hctx,
                            cls_fifo_data_obj_header *part_header)
{
  bufferlist bl;
  int r = cls_cxx_getxattr(hctx, part_header_xattr_name, &bl);
  if (r < 0) {
    if (r != -ENOENT &&
        r != -ENODATA) {
      CLS_ERR("ERROR: %s(): cls_cxx_getxattr(%s) returned r=%d", __func__, part_header_xattr_name, r);
    }
    return r;
  }

  auto iter = bl.cbegin();
  try {
    decode(*part_header, iter);
  } catch (buffer::error& err) {
    CLS_ERR("ERROR: %s(): failed decoding part header", __func__);
    return -EIO;
  }

  return 0;

}

static int write_part_header(cls_method_context_t hctx,
                             cls_fifo_data_obj_header& part_header)
{
  bufferlist bl;
  encode(part_header, bl);

  return cls_cxx_setxattr(hctx, part_header_xattr_name, &bl);
}

static int read_header(cls_method_context_t hctx,
                       std::optional<fifo_objv_t> objv,
                       fifo_info_t *info)
{
  uint64_t size;

  int r = cls_cxx_stat2(hctx, &size, nullptr);
  if (r < 0) {
    CLS_ERR("ERROR: %s(): cls_cxx_stat2() on obj returned %d", __func__, r);
    return r;
  }

  bufferlist bl;
  r = cls_cxx_read2(hctx, 0, size, &bl, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (r < 0) {
    CLS_ERR("ERROR: %s(): cls_cxx_read2() on obj returned %d", __func__, r);
    return r;
  }

  try {
    auto iter = bl.cbegin();
    decode(*info, iter);
  } catch (buffer::error& err) {
    CLS_ERR("ERROR: %s(): failed decoding header", __func__);
    return -EIO;
  }

  if (objv &&
      !(info->objv == *objv)) {
    string s1 = info->objv.to_str();
    string s2 = objv->to_str();
    CLS_LOG(10, "%s(): version mismatch (header=%s, req=%s), cancelled operation", __func__, s1.c_str(), s2.c_str());
    return -ECANCELED;
  }

  return 0;
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

    fifo_info_t header;
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
           header.oid_prefix == *op.oid_prefix) &&
          (!op.objv ||
           header.objv == *op.objv))) {
      CLS_LOG(10, "%s(): failed to re-create existing queue with different params", __func__);
      return -EEXIST;
    }

    return 0; /* already exists */
  }
  fifo_info_t header;
  
  header.id = op.id;
  if (op.objv) {
    header.objv = *op.objv;
  }
  header.oid_prefix = new_oid_prefix(op.id, op.oid_prefix);
  header.pool.name = op.pool.name;
  header.pool.ns = op.pool.ns;

  header.data_params.max_obj_size = op.max_obj_size;
  header.data_params.max_entry_size = op.max_entry_size;
  header.data_params.full_size_threshold = op.max_obj_size - op.max_entry_size;

  r = write_header(hctx, header);
  if (r < 0) {
    CLS_LOG(10, "%s(): failed to write header: r=%d", __func__, r);
    return r;
  }

  return 0;
}

static int fifo_get_info_op(cls_method_context_t hctx,
                          bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "%s", __func__);

  cls_fifo_get_info_op op;
  try {
    auto iter = in->cbegin();
    decode(op, iter);
  } catch (const buffer::error &err) {
    CLS_ERR("ERROR: %s(): failed to decode request", __func__);
    return -EINVAL;
  }

  cls_fifo_get_info_op_reply reply;
  int r = read_header(hctx, op.objv, &reply.info);
  if (r < 0) {
    return r;
  }

  encode(reply, *out);

  return 0;
}

static int fifo_init_part_op(cls_method_context_t hctx,
                             bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "%s", __func__);

  cls_fifo_init_part_op op;
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
  if (r == 0 && size > 0) {
    cls_fifo_data_obj_header part_header;
    r = read_part_header(hctx, &part_header);
    if (r < 0) {
      CLS_LOG(10, "%s(): failed to read part header", __func__);
      return r;
    }

    if (!(part_header.tag == op.tag &&
          part_header.params == op.data_params)) {
      CLS_LOG(10, "%s(): failed to re-create existing part with different params", __func__);
      return -EEXIST;
    }

    return 0; /* already exists */
  }

  cls_fifo_data_obj_header part_header;
  
  part_header.tag = op.tag;
  part_header.params = op.data_params;

  r = write_part_header(hctx, part_header);
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
  cls_method_handle_t h_fifo_get_info_op;
  cls_method_handle_t h_fifo_init_part_op;

  cls_register("fifo", &h_class);
  cls_register_cxx_method(h_class, "fifo_create",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          fifo_create_op, &h_fifo_create_op);

  cls_register_cxx_method(h_class, "fifo_get_info",
                          CLS_METHOD_RD,
                          fifo_get_info_op, &h_fifo_get_info_op);

  cls_register_cxx_method(h_class, "fifo_init_part",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          fifo_init_part_op, &h_fifo_init_part_op);

  return;
}
