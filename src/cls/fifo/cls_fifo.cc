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


#define CLS_FIFO_MAX_PART_HEADER_SIZE 512

static uint32_t part_entry_overhead;

struct cls_fifo_entry_header_pre {
  __le64 magic;
  __le64 pre_size;
  __le64 header_size;
  __le64 data_size;
  __le64 index;
  __le32 reserved;
} __attribute__ ((packed));

struct cls_fifo_entry_header {
  ceph::real_time mtime;

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(mtime, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(1, bl);
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
                        fifo_info_t& header,
                        bool inc_ver = true)
{
  if (header.objv.instance.empty()) {
#define HEADER_INSTANCE_SIZE 16
  char buf[HEADER_INSTANCE_SIZE + 1];
  buf[HEADER_INSTANCE_SIZE] = 0;
  cls_gen_rand_base64(buf, sizeof(buf) - 1);

    header.objv.instance = buf;
  }
  if (inc_ver) {
    ++header.objv.ver;
  }
  bufferlist bl;
  encode(header, bl);
  return cls_cxx_write_full(hctx, &bl);
}

static int read_part_header(cls_method_context_t hctx,
                            fifo_part_header_t *part_header)
{
  bufferlist bl;
  int r = cls_cxx_read2(hctx, 0, CLS_FIFO_MAX_PART_HEADER_SIZE, &bl, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (r < 0) {
    CLS_ERR("ERROR: %s(): cls_cxx_read2() on obj returned %d", __func__, r);
    return r;
  }

  auto iter = bl.cbegin();
  try {
    decode(*part_header, iter);
  } catch (buffer::error& err) {
    CLS_ERR("ERROR: %s(): failed decoding part header", __func__);
    return -EIO;
  }

  CLS_LOG(20, "%s():%d read part_header:\n"
           "\ttag=%s\n"
           "\tmagic=0x%llx\n"
           "\tmin_ofs=%lld\n"
           "\tmax_ofs=%lld\n"
           "\tmin_index=%lld\n"
           "\tmax_index=%lld\n",
           __func__, __LINE__,
           part_header->tag.c_str(), 
           (long long)part_header->magic,
           (long long)part_header->min_ofs,
           (long long)part_header->max_ofs,
           (long long)part_header->min_index,
           (long long)part_header->max_index);

  return 0;

}

static int write_part_header(cls_method_context_t hctx,
                             fifo_part_header_t& part_header)
{
  bufferlist bl;
  encode(part_header, bl);

  if (bl.length() > CLS_FIFO_MAX_PART_HEADER_SIZE) {
    CLS_LOG(10, "%s(): cannot write part header, buffer exceeds max size", __func__);
    return -EIO;
  }

  int r = cls_cxx_write2(hctx, 0, bl.length(),
                     &bl, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (r < 0) {
    CLS_LOG(10, "%s(): failed to write part header: r=%d",
            __func__, r);
    return r;
  }

  return 0;
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

static int fifo_meta_create_op(cls_method_context_t hctx,
                          bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "%s", __func__);

  cls_fifo_meta_create_op op;
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
  } else {
#define DEFAULT_INSTANCE_SIZE 16
    char buf[DEFAULT_INSTANCE_SIZE + 1];
    cls_gen_rand_base64(buf, sizeof(buf));
    buf[DEFAULT_INSTANCE_SIZE] = '\0';
    header.objv.instance = buf;
    header.objv.ver = 1;
  }
  header.oid_prefix = new_oid_prefix(op.id, op.oid_prefix);

  header.data_params.max_part_size = op.max_part_size;
  header.data_params.max_entry_size = op.max_entry_size;
  header.data_params.full_size_threshold = op.max_part_size - op.max_entry_size - part_entry_overhead;

  r = write_header(hctx, header, false);
  if (r < 0) {
    CLS_LOG(10, "%s(): failed to write header: r=%d", __func__, r);
    return r;
  }

  return 0;
}

static int fifo_meta_update_op(cls_method_context_t hctx,
                                bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "%s", __func__);

  cls_fifo_meta_update_op op;
  try {
    auto iter = in->cbegin();
    decode(op, iter);
  } catch (const buffer::error &err) {
    CLS_ERR("ERROR: %s(): failed to decode request", __func__);
    return -EINVAL;
  }

  fifo_info_t header;

  int r = read_header(hctx, op.objv, &header);
  if (r < 0) {
    return r;
  }

  string err;

  r = header.apply_update(op.tail_part_num,
                          op.head_part_num,
                          op.min_push_part_num,
                          op.max_push_part_num,
                          op.journal_entries_add,
                          op.journal_entries_rm,
                          &err);
  if (r < 0) {
    CLS_LOG(10, "%s(): %s", __func__, err.c_str());
    return r;
  }

  r = write_header(hctx, header);
  if (r < 0) {
    CLS_LOG(10, "%s(): failed to write header: r=%d", __func__, r);
    return r;
  }

  return 0;
}

static int fifo_meta_get_op(cls_method_context_t hctx,
                          bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "%s", __func__);

  cls_fifo_meta_get_op op;
  try {
    auto iter = in->cbegin();
    decode(op, iter);
  } catch (const buffer::error &err) {
    CLS_ERR("ERROR: %s(): failed to decode request", __func__);
    return -EINVAL;
  }

  cls_fifo_meta_get_op_reply reply;
  int r = read_header(hctx, op.objv, &reply.info);
  if (r < 0) {
    return r;
  }

  reply.part_header_size = CLS_FIFO_MAX_PART_HEADER_SIZE;
  reply.part_entry_overhead = part_entry_overhead;

  encode(reply, *out);

  return 0;
}

static int fifo_part_init_op(cls_method_context_t hctx,
                             bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "%s", __func__);

  cls_fifo_part_init_op op;
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
    fifo_part_header_t part_header;
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

  fifo_part_header_t part_header;
  
  part_header.tag = op.tag;
  part_header.params = op.data_params;

  part_header.min_ofs = CLS_FIFO_MAX_PART_HEADER_SIZE;
  part_header.max_ofs = part_header.min_ofs;

  cls_gen_random_bytes((char *)&part_header.magic, sizeof(part_header.magic));

  r = write_part_header(hctx, part_header);
  if (r < 0) {
    CLS_LOG(10, "%s(): failed to write header: r=%d", __func__, r);
    return r;
  }

  return 0;
}

static bool full_part(const fifo_part_header_t& part_header)
{
  return (part_header.max_ofs > part_header.params.full_size_threshold);
}

static int fifo_part_push_op(cls_method_context_t hctx,
                             bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "%s", __func__);

  cls_fifo_part_push_op op;
  try {
    auto iter = in->cbegin();
    decode(op, iter);
  } catch (const buffer::error &err) {
    CLS_ERR("ERROR: %s(): failed to decode request", __func__);
    return -EINVAL;
  }

  fifo_part_header_t part_header;
  int r = read_part_header(hctx, &part_header);
  if (r < 0) {
    CLS_LOG(10, "%s(): failed to read part header", __func__);
    return r;
  }

  if (!(part_header.tag == op.tag)) {
    CLS_LOG(10, "%s(): bad tag", __func__);
    return -EINVAL;
  }

  uint64_t effective_len = op.total_len + op.data_bufs.size() * part_entry_overhead;

  if (effective_len > part_header.params.max_entry_size + part_entry_overhead) {
    return -EINVAL;
  }

  if (full_part(part_header)) {
    return -ERANGE;
  }

  struct cls_fifo_entry_header entry_header;
  entry_header.mtime = real_clock::now();

  bufferlist entry_header_bl;
  encode(entry_header, entry_header_bl);

  auto max_index = part_header.max_index;
  auto ofs = part_header.max_ofs;

  cls_fifo_entry_header_pre pre_header;
  pre_header.magic = part_header.magic;
  pre_header.pre_size = sizeof(pre_header);
  pre_header.reserved = 0;

  uint64_t total_data = 0;

  for (auto& data : op.data_bufs) {
    total_data += data.length();

    pre_header.header_size = entry_header_bl.length();
    pre_header.data_size = data.length();
    pre_header.index = max_index;

    bufferptr pre((char *)&pre_header, sizeof(pre_header));
    bufferlist all_data;
    all_data.append(pre);
    all_data.append(entry_header_bl);
    all_data.claim_append(data);

    auto write_len = all_data.length();

    r = cls_cxx_write2(hctx, ofs, write_len,
                       &all_data, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
    if (r < 0) {
      CLS_LOG(10, "%s(): failed to write entry (ofs=%lld len=%lld): r=%d",
              __func__, (long long)part_header.max_ofs, (long long)write_len, r);
      return r;
    }

    ofs += write_len;
    ++max_index;
  }

  if (total_data != op.total_len) {
    CLS_LOG(10, "%s(): length mismatch: op.total_len=%lld total data received=%lld",
            __func__, (long long)op.total_len, (long long)total_data);
    return -EINVAL;
  }

  part_header.max_index = max_index;
  part_header.max_ofs = ofs;

  r = write_part_header(hctx, part_header);
  if (r < 0) {
    CLS_LOG(10, "%s(): failed to write header: r=%d", __func__, r);
    return r;
  }

  return 0;
}

class EntryReader {
  static constexpr uint64_t prefetch_len = (128 * 1024);

  cls_method_context_t hctx;

  fifo_part_header_t& part_header;

  uint64_t ofs;
  bufferlist data;

  int fetch(uint64_t num_bytes);
  int read(uint64_t num_bytes, bufferlist *pbl);
  int peek(uint64_t num_bytes, char *dest);
  int seek(uint64_t num_bytes);

public:
  EntryReader(cls_method_context_t _hctx,
              fifo_part_header_t& _part_header,
              uint64_t _ofs) : hctx(_hctx),
                               part_header(_part_header),
                               ofs(_ofs) {
    if (ofs < part_header.min_ofs) {
      ofs = part_header.min_ofs;
    }
  }

  uint64_t get_ofs() const {
    return ofs;
  }

  bool end() const {
    return (ofs >= part_header.max_ofs);
  }

  int peek_pre_header(cls_fifo_entry_header_pre *pre_header);
  int get_next_entry(bufferlist *pbl,
                     uint64_t *pofs,
                     ceph::real_time *pmtime);
};


int EntryReader::fetch(uint64_t num_bytes)
{
  CLS_LOG(20, "%s(): fetch %d bytes, ofs=%d data.length()=%d", __func__, (int)num_bytes, (int)ofs, (int)data.length());
  if (data.length() < num_bytes) {
    bufferlist bl;
    CLS_LOG(20, "%s(): reading %d bytes at ofs=%d", __func__, (int)prefetch_len, (int)ofs + data.length());
    int r = cls_cxx_read2(hctx, ofs + data.length(), prefetch_len, &bl, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
    if (r < 0) {
      CLS_ERR("ERROR: %s(): cls_cxx_read2() on obj returned %d", __func__, r);
      return r;
    }
    data.claim_append(bl);
  }

  if ((unsigned)num_bytes > data.length()) {
    CLS_LOG(20, "%s(): requested %lld bytes, but only %lld were available", __func__, (long long)num_bytes, (long long)data.length());
    return -ERANGE;
  }

  return 0;
}

int EntryReader::read(uint64_t num_bytes, bufferlist *pbl)
{
  int r = fetch(num_bytes);
  if (r < 0) {
    return r;
  }
  data.splice(0, num_bytes, pbl);

  ofs += num_bytes;

  return 0;
}

int EntryReader::peek(uint64_t num_bytes, char *dest)
{
  int r = fetch(num_bytes);
  if (r < 0) {
    return r;
  }

  data.begin().copy(num_bytes, dest);

  return 0;
}

int EntryReader::seek(uint64_t num_bytes)
{
  bufferlist bl;

  CLS_LOG(20, "%s():%d: num_bytes=%d", __func__, __LINE__, (int)num_bytes);
  return read(num_bytes, &bl);
}

int EntryReader::peek_pre_header(cls_fifo_entry_header_pre *pre_header)
{
  if (end()) {
    return -ENOENT;
  }

  int r = peek(sizeof(*pre_header), (char *)pre_header);
  if (r < 0) {
    CLS_ERR("ERROR: %s(): peek() size=%d failed: r=%d", __func__, (int)sizeof(pre_header), r);
    return r;
  }

  if (pre_header->magic != part_header.magic) {
    CLS_ERR("ERROR: %s(): unexpected pre_header magic", __func__);
    return -ERANGE;
  }

  return 0;
}


int EntryReader::get_next_entry(bufferlist *pbl,
                                uint64_t *pofs,
                                ceph::real_time *pmtime)
{
  cls_fifo_entry_header_pre pre_header;
  int r = peek_pre_header(&pre_header);
  if (r < 0) {
    CLS_ERR("ERROR: %s(): peek_pre_header() failed: r=%d", __func__, r);
    return r;
  }

  if (pofs) {
    *pofs = ofs;
  }

  CLS_LOG(20, "%s():%d: pre_header.pre_size=%d", __func__, __LINE__, (int)pre_header.pre_size);
  r = seek(pre_header.pre_size);
  if (r < 0) {
    CLS_ERR("ERROR: %s(): failed to seek: r=%d", __func__, r);
    return r;
  }

  bufferlist header;
  CLS_LOG(20, "%s():%d: pre_header.header_size=%d", __func__, __LINE__, (int)pre_header.header_size);
  r = read(pre_header.header_size, &header);
  if (r < 0) {
    CLS_ERR("ERROR: %s(): failed to read entry header: r=%d", __func__, r);
    return r;
  }

  cls_fifo_entry_header entry_header;
  auto iter = header.cbegin();
  try {
    decode(entry_header, iter);
  } catch (buffer::error& err) {
    CLS_ERR("%s(): failed decoding entry header", __func__);
    return -EIO;
  }

  if (pmtime) {
    *pmtime = entry_header.mtime;
  }

  if (pbl) {
    r = read(pre_header.data_size, pbl);
    if (r < 0) {
      CLS_ERR("%s(): failed reading data: r=%d", __func__, r);
      return r;
    }
  } else {
    r = seek(pre_header.data_size);
    if (r < 0) {
      CLS_ERR("ERROR: %s(): failed to seek: r=%d", __func__, r);
      return r;
    }
  }

  return 0;
}

static int fifo_part_trim_op(cls_method_context_t hctx,
                             bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "%s", __func__);

  cls_fifo_part_trim_op op;
  try {
    auto iter = in->cbegin();
    decode(op, iter);
  } catch (const buffer::error &err) {
    CLS_ERR("ERROR: %s(): failed to decode request", __func__);
    return -EINVAL;
  }

  fifo_part_header_t part_header;
  int r = read_part_header(hctx, &part_header);
  if (r < 0) {
    CLS_LOG(10, "%s(): failed to read part header", __func__);
    return r;
  }

  if (op.tag &&
      !(part_header.tag == *op.tag)) {
    CLS_LOG(10, "%s(): bad tag", __func__);
    return -EINVAL;
  }

  if (op.ofs < part_header.min_ofs) {
    return 0;
  }

  if (op.ofs >= part_header.max_ofs) {
    if (full_part(part_header)) {
      /*
       * trim full part completely: remove object
       */

      r = cls_cxx_remove(hctx);
      if (r < 0) {
        CLS_LOG(0, "%s(): ERROR: cls_cxx_remove() returned r=%d", __func__, r);
        return r;
      }

      return 0;
    }
    
    part_header.min_ofs = part_header.max_ofs;
    part_header.min_index = part_header.max_index;
  } else {
    EntryReader reader(hctx, part_header, op.ofs);

    cls_fifo_entry_header_pre pre_header;
    int r = reader.peek_pre_header(&pre_header);
    if (r < 0) {
      return r;
    }

    r = reader.get_next_entry(nullptr, nullptr, nullptr);
    if (r < 0) {
      CLS_ERR("ERROR: %s(): unexpected failure at get_next_entry(): r=%d", __func__, r);
      return r;
    }

    part_header.min_ofs = reader.get_ofs();
    part_header.min_index = pre_header.index + 1;
  }

  r = write_part_header(hctx, part_header);
  if (r < 0) {
    CLS_LOG(10, "%s(): failed to write header: r=%d", __func__, r);
    return r;
  }

  return 0;
}

static int fifo_part_list_op(cls_method_context_t hctx,
                             bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "%s", __func__);

  cls_fifo_part_list_op op;
  try {
    auto iter = in->cbegin();
    decode(op, iter);
  } catch (const buffer::error &err) {
    CLS_ERR("ERROR: %s(): failed to decode request", __func__);
    return -EINVAL;
  }

  fifo_part_header_t part_header;
  int r = read_part_header(hctx, &part_header);
  if (r < 0) {
    CLS_LOG(10, "%s(): failed to read part header", __func__);
    return r;
  }

  if (op.tag &&
      !(part_header.tag == *op.tag)) {
    CLS_LOG(10, "%s(): bad tag", __func__);
    return -EINVAL;
  }

  EntryReader reader(hctx, part_header, op.ofs);

  if (op.ofs >= part_header.min_ofs &&
      !reader.end()) {
    r = reader.get_next_entry(nullptr, nullptr, nullptr);
    if (r < 0) {
      CLS_ERR("ERROR: %s(): unexpected failure at get_next_entry(): r=%d", __func__, r);
      return r;
    }
  }

  cls_fifo_part_list_op_reply reply;

  reply.tag = part_header.tag;

#define LIST_MAX_ENTRIES 512

  auto max_entries = std::min(op.max_entries, (int)LIST_MAX_ENTRIES);

  for (int i = 0; i < max_entries && !reader.end(); ++i) {
    bufferlist data;
    ceph::real_time mtime;
    uint64_t ofs;

    r = reader.get_next_entry(&data, &ofs, &mtime);
    if (r < 0) {
      CLS_ERR("ERROR: %s(): unexpected failure at get_next_entry(): r=%d", __func__, r);
      return r;
    }

    reply.entries.emplace_back(std::move(data), ofs, mtime);
  }

  reply.more = !reader.end();
  reply.full_part = full_part(part_header);

  encode(reply, *out);

  return 0;
}

static int fifo_part_get_info_op(cls_method_context_t hctx,
                                 bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "%s", __func__);

  cls_fifo_part_get_info_op op;
  try {
    auto iter = in->cbegin();
    decode(op, iter);
  } catch (const buffer::error &err) {
    CLS_ERR("ERROR: %s(): failed to decode request", __func__);
    return -EINVAL;
  }

  cls_fifo_part_get_info_op_reply reply;

  int r = read_part_header(hctx, &reply.header);
  if (r < 0) {
    CLS_LOG(10, "%s(): failed to read part header", __func__);
    return r;
  }

  encode(reply, *out);

  return 0;
}

CLS_INIT(fifo)
{
  CLS_LOG(20, "Loaded fifo class!");

  cls_handle_t h_class;
  cls_method_handle_t h_fifo_meta_create_op;
  cls_method_handle_t h_fifo_meta_get_op;
  cls_method_handle_t h_fifo_meta_update_op;
  cls_method_handle_t h_fifo_part_init_op;
  cls_method_handle_t h_fifo_part_push_op;
  cls_method_handle_t h_fifo_part_trim_op;
  cls_method_handle_t h_fifo_part_list_op;
  cls_method_handle_t h_fifo_part_get_info_op;

  cls_register("fifo", &h_class);
  cls_register_cxx_method(h_class, "fifo_meta_create",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          fifo_meta_create_op, &h_fifo_meta_create_op);

  cls_register_cxx_method(h_class, "fifo_meta_get",
                          CLS_METHOD_RD,
                          fifo_meta_get_op, &h_fifo_meta_get_op);

  cls_register_cxx_method(h_class, "fifo_meta_update",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          fifo_meta_update_op, &h_fifo_meta_update_op);

  cls_register_cxx_method(h_class, "fifo_part_init",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          fifo_part_init_op, &h_fifo_part_init_op);

  cls_register_cxx_method(h_class, "fifo_part_push",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          fifo_part_push_op, &h_fifo_part_push_op);

  cls_register_cxx_method(h_class, "fifo_part_trim",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          fifo_part_trim_op, &h_fifo_part_trim_op);

  cls_register_cxx_method(h_class, "fifo_part_list",
                          CLS_METHOD_RD,
                          fifo_part_list_op, &h_fifo_part_list_op);

  cls_register_cxx_method(h_class, "fifo_part_get_info",
                          CLS_METHOD_RD,
                          fifo_part_get_info_op, &h_fifo_part_get_info_op);

  /* calculate entry overhead */
  struct cls_fifo_entry_header entry_header;
  bufferlist entry_header_bl;
  encode(entry_header, entry_header_bl);

  part_entry_overhead = sizeof(cls_fifo_entry_header_pre) + entry_header_bl.length();

  return;
}
