// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/** \file
 *
 * This is an OSD class that implements methods for management
 * and use of fifo
 *
 */

#include <cerrno>
#include <optional>
#include <string>

#undef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY 1
#include <fmt/format.h>

#include "include/buffer.h"
#include "include/types.h"

#include "objclass/objclass.h"

#include "cls/fifo/cls_fifo_ops.h"
#include "cls/fifo/cls_fifo_types.h"

CLS_VER(1,0)
CLS_NAME(fifo)

namespace rados::cls::fifo {

static constexpr auto CLS_FIFO_MAX_PART_HEADER_SIZE = 512;

static std::uint32_t part_entry_overhead;

struct entry_header_pre {
  __le64 magic;
  __le64 pre_size;
  __le64 header_size;
  __le64 data_size;
  __le64 index;
  __le32 reserved;
} __attribute__ ((packed));

struct entry_header {
  ceph::real_time mtime;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(mtime, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(mtime, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(entry_header)

namespace {

std::string new_oid_prefix(std::string id, std::optional<std::string>& val)
{
  static constexpr auto PREFIX_RND_SIZE = 12;
  if (val) {
    return *val;
  }

  char buf[PREFIX_RND_SIZE + 1];
  buf[PREFIX_RND_SIZE] = 0;

  cls_gen_rand_base64(buf, sizeof(buf) - 1);

  return fmt::format("{}.{}", id, buf);
}

int write_header(cls_method_context_t hctx,
		 info& header,
		 bool inc_ver = true)
{
  static constexpr auto HEADER_INSTANCE_SIZE = 16;
  if (header.version.instance.empty()) {
    char buf[HEADER_INSTANCE_SIZE + 1];
    buf[HEADER_INSTANCE_SIZE] = 0;
    cls_gen_rand_base64(buf, sizeof(buf) - 1);
    header.version.instance = buf;
  }
  if (inc_ver) {
    ++header.version.ver;
  }
  ceph::buffer::list bl;
  encode(header, bl);
  return cls_cxx_write_full(hctx, &bl);
}

int read_part_header(cls_method_context_t hctx,
		     part_header* part_header)
{
  ceph::buffer::list bl;
  int r = cls_cxx_read2(hctx, 0, CLS_FIFO_MAX_PART_HEADER_SIZE, &bl,
			CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (r < 0) {
    CLS_ERR("ERROR: %s(): cls_cxx_read2() on obj returned %d", __func__, r);
    return r;
  }

  auto iter = bl.cbegin();
  try {
    decode(*part_header, iter);
  } catch (const ceph::buffer::error& err) {
    CLS_ERR("ERROR: %s(): failed decoding part header", __func__);
    return -EIO;
  }

  using ceph::operator <<;
  std::ostringstream ss;
  ss << part_header->max_time;
  CLS_LOG(10, "%s():%d read part_header:\n"
	  "\ttag=%s\n"
	  "\tmagic=0x%" PRIx64 "\n"
	  "\tmin_ofs=%" PRId64 "\n"
	  "\tlast_ofs=%" PRId64 "\n"
	  "\tnext_ofs=%" PRId64 "\n"
	  "\tmin_index=%" PRId64 "\n"
	  "\tmax_index=%" PRId64 "\n"
	  "\tmax_time=%s\n",
	  __func__, __LINE__,
	  part_header->tag.c_str(),
	  part_header->magic,
	  part_header->min_ofs,
	  part_header->last_ofs,
	  part_header->next_ofs,
	  part_header->min_index,
	  part_header->max_index,
	  ss.str().c_str());

  return 0;
}

int write_part_header(cls_method_context_t hctx,
		      part_header& part_header)
{
  ceph::buffer::list bl;
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

int read_header(cls_method_context_t hctx,
		std::optional<objv> objv,
		info* info)
{
  std::uint64_t size;

  int r = cls_cxx_stat2(hctx, &size, nullptr);
  if (r < 0) {
    CLS_ERR("ERROR: %s(): cls_cxx_stat2() on obj returned %d", __func__, r);
    return r;
  }

  ceph::buffer::list bl;
  r = cls_cxx_read2(hctx, 0, size, &bl, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (r < 0) {
    CLS_ERR("ERROR: %s(): cls_cxx_read2() on obj returned %d", __func__, r);
    return r;
  }

  if (r == 0) {
    CLS_ERR("ERROR: %s(): Zero length object, returning ENODATA", __func__);
    return -ENODATA;
  }

  try {
    auto iter = bl.cbegin();
    decode(*info, iter);
  } catch (const ceph::buffer::error& err) {
    CLS_ERR("ERROR: %s(): failed decoding header", __func__);
    return -EIO;
  }

  if (objv && !(info->version== *objv)) {
    auto s1 = info->version.to_str();
    auto s2 = objv->to_str();
    CLS_LOG(10, "%s(): version mismatch (header=%s, req=%s), canceled operation",
	    __func__, s1.c_str(), s2.c_str());
    return -ECANCELED;
  }

  return 0;
}

int create_meta(cls_method_context_t hctx,
		ceph::buffer::list* in, ceph::buffer::list* out)
{
  CLS_LOG(10, "%s", __func__);

  op::create_meta op;
  try {
    auto iter = in->cbegin();
    decode(op, iter);
  } catch (const ceph::buffer::error& err) {
    CLS_ERR("ERROR: %s(): failed to decode request", __func__);
    return -EINVAL;
  }

  if (op.id.empty()) {
    CLS_LOG(10, "%s(): ID cannot be empty", __func__);
    return -EINVAL;
  }

  if (op.max_part_size == 0 ||
      op.max_entry_size == 0 ||
      op.max_entry_size > op.max_part_size) {
    CLS_ERR("ERROR: %s(): invalid dimensions.", __func__);
    return -EINVAL;
  }

  std::uint64_t size;

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
    ceph::buffer::list bl;
    r = cls_cxx_read2(hctx, 0, size, &bl, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
    if (r < 0) {
      CLS_ERR("ERROR: %s(): cls_cxx_read2() on obj returned %d", __func__, r);
      return r;
    }

    info header;
    try {
      auto iter = bl.cbegin();
      decode(header, iter);
    } catch (const ceph::buffer::error& err) {
      CLS_ERR("ERROR: %s(): failed decoding header", __func__);
      return -EIO;
    }

    if (!(header.id == op.id &&
          (!op.oid_prefix ||
           header.oid_prefix == *op.oid_prefix) &&
          (!op.version ||
           header.version == *op.version))) {
      CLS_LOG(10, "%s(): failed to re-create existing queue "
	      "with different params", __func__);
      return -EEXIST;
    }

    return 0; /* already exists */
  }
  info header;

  header.id = op.id;
  if (op.version) {
    header.version = *op.version;
  } else {
    static constexpr auto DEFAULT_INSTANCE_SIZE = 16;
    char buf[DEFAULT_INSTANCE_SIZE + 1];
    cls_gen_rand_base64(buf, sizeof(buf));
    buf[DEFAULT_INSTANCE_SIZE] = '\0';
    header.version.instance = buf;
    header.version.ver = 1;
  }
  header.oid_prefix = new_oid_prefix(op.id, op.oid_prefix);

  header.params.max_part_size = op.max_part_size;
  header.params.max_entry_size = op.max_entry_size;
  header.params.full_size_threshold = op.max_part_size - op.max_entry_size - part_entry_overhead;

  r = write_header(hctx, header, false);
  if (r < 0) {
    CLS_LOG(10, "%s(): failed to write header: r=%d", __func__, r);
    return r;
  }

  return 0;
}

int update_meta(cls_method_context_t hctx, ceph::buffer::list* in,
		ceph::buffer::list* out)
{
  CLS_LOG(10, "%s", __func__);

  op::update_meta op;
  try {
    auto iter = in->cbegin();
    decode(op, iter);
  } catch (const ceph::buffer::error& err) {
    CLS_ERR("ERROR: %s(): failed to decode request", __func__);
    return -EINVAL;
  }

  if (op.version.empty()) {
    CLS_LOG(10, "%s(): no version supplied", __func__);
    return -EINVAL;
  }

  info header;

  int r = read_header(hctx, op.version, &header);
  if (r < 0) {
    return r;
  }

  auto err = header.apply_update(fifo::update()
				 .tail_part_num(op.tail_part_num)
				 .head_part_num(op.head_part_num)
				 .min_push_part_num(op.min_push_part_num)
				 .max_push_part_num(op.max_push_part_num)
				 .journal_entries_add(
				   std::move(op.journal_entries_add))
				 .journal_entries_rm(
				   std::move(op.journal_entries_rm)));
  if (err) {
    CLS_LOG(10, "%s(): %s", __func__, err->c_str());
    return -EINVAL;
  }

  r = write_header(hctx, header);
  if (r < 0) {
    CLS_LOG(10, "%s(): failed to write header: r=%d", __func__, r);
    return r;
  }

  return 0;
}

int get_meta(cls_method_context_t hctx, ceph::buffer::list* in,
	     ceph::buffer::list* out)
{
  CLS_LOG(10, "%s", __func__);

  op::get_meta op;
  try {
    auto iter = in->cbegin();
    decode(op, iter);
  } catch (const ceph::buffer::error &err) {
    CLS_ERR("ERROR: %s(): failed to decode request", __func__);
    return -EINVAL;
  }

  op::get_meta_reply reply;
  int r = read_header(hctx, op.version, &reply.info);
  if (r < 0) {
    return r;
  }

  reply.part_header_size = CLS_FIFO_MAX_PART_HEADER_SIZE;
  reply.part_entry_overhead = part_entry_overhead;

  encode(reply, *out);

  return 0;
}

int init_part(cls_method_context_t hctx, ceph::buffer::list* in,
	      ceph::buffer::list *out)
{
  CLS_LOG(10, "%s", __func__);

  op::init_part op;
  try {
    auto iter = in->cbegin();
    decode(op, iter);
  } catch (const ceph::buffer::error &err) {
    CLS_ERR("ERROR: %s(): failed to decode request", __func__);
    return -EINVAL;
  }

  std::uint64_t size;

  if (op.tag.empty()) {
    CLS_LOG(10, "%s(): tag required", __func__);
    return -EINVAL;
  }

  int r = cls_cxx_stat2(hctx, &size, nullptr);
  if (r < 0 && r != -ENOENT) {
    CLS_ERR("ERROR: %s(): cls_cxx_stat2() on obj returned %d", __func__, r);
    return r;
  }
  if (r == 0 && size > 0) {
    part_header part_header;
    r = read_part_header(hctx, &part_header);
    if (r < 0) {
      CLS_LOG(10, "%s(): failed to read part header", __func__);
      return r;
    }

    if (!(part_header.tag == op.tag &&
          part_header.params == op.params)) {
      CLS_LOG(10, "%s(): failed to re-create existing part with different "
	      "params", __func__);
      return -EEXIST;
    }

    return 0; /* already exists */
  }

  part_header part_header;

  part_header.tag = op.tag;
  part_header.params = op.params;

  part_header.min_ofs = CLS_FIFO_MAX_PART_HEADER_SIZE;
  part_header.last_ofs = 0;
  part_header.next_ofs = part_header.min_ofs;
  part_header.max_time = ceph::real_clock::now();

  cls_gen_random_bytes(reinterpret_cast<char *>(&part_header.magic),
		       sizeof(part_header.magic));

  r = write_part_header(hctx, part_header);
  if (r < 0) {
    CLS_LOG(10, "%s(): failed to write header: r=%d", __func__, r);
    return r;
  }

  return 0;
}

bool full_part(const part_header& part_header)
{
  return (part_header.next_ofs > part_header.params.full_size_threshold);
}

int push_part(cls_method_context_t hctx, ceph::buffer::list* in,
	      ceph::buffer::list* out)
{
  CLS_LOG(10, "%s", __func__);

  op::push_part op;
  try {
    auto iter = in->cbegin();
    decode(op, iter);
  } catch (const ceph::buffer::error& err) {
    CLS_ERR("ERROR: %s(): failed to decode request", __func__);
    return -EINVAL;
  }

  if (op.tag.empty()) {
    CLS_LOG(10, "%s(): tag required", __func__);
    return -EINVAL;
  }

  part_header part_header;
  int r = read_part_header(hctx, &part_header);
  if (r < 0) {
    CLS_LOG(10, "%s(): failed to read part header", __func__);
    return r;
  }

  if (!(part_header.tag == op.tag)) {
    CLS_LOG(10, "%s(): bad tag", __func__);
    return -EINVAL;
  }

  std::uint64_t effective_len = op.total_len + op.data_bufs.size() *
    part_entry_overhead;

  if (effective_len > part_header.params.max_part_size) {
    return -EINVAL;
  }

  if (full_part(part_header)) {
    return -ERANGE;
  }

  auto now = ceph::real_clock::now();
  struct entry_header entry_header = { now };
  ceph::buffer::list entry_header_bl;
  encode(entry_header, entry_header_bl);

  auto max_index = part_header.max_index;
  const auto write_ofs = part_header.next_ofs;
  auto ofs = part_header.next_ofs;

  entry_header_pre pre_header;
  pre_header.magic = part_header.magic;
  pre_header.pre_size = sizeof(pre_header);
  pre_header.reserved = 0;

  std::uint64_t total_data = 0;
  for (auto& data : op.data_bufs) {
    total_data += data.length();
  }
  if (total_data != op.total_len) {
    CLS_LOG(10, "%s(): length mismatch: op.total_len=%" PRId64
	    " total data received=%" PRId64,
            __func__, op.total_len, total_data);
    return -EINVAL;
  }


  int entries_pushed = 0;
  ceph::buffer::list all_data;
  for (auto& data : op.data_bufs) {
    if (full_part(part_header))
      break;

    pre_header.header_size = entry_header_bl.length();
    pre_header.data_size = data.length();
    pre_header.index = max_index;

    bufferptr pre(reinterpret_cast<char*>(&pre_header), sizeof(pre_header));
    auto entry_write_len = pre.length() + entry_header_bl.length() + data.length();
    all_data.append(pre);
    all_data.append(entry_header_bl);
    all_data.claim_append(data);

    part_header.last_ofs = ofs;
    ofs += entry_write_len;
    ++max_index;
    ++entries_pushed;
    part_header.max_index = max_index;
    part_header.next_ofs = ofs;
  }
  part_header.max_time = now;

  auto write_len = all_data.length();

  r = cls_cxx_write2(hctx, write_ofs, write_len,
		     &all_data, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);

  if (r < 0) {
    CLS_LOG(10,"%s(): failed to write entries (ofs=%" PRIu64
	    " len=%u): r=%d", __func__, write_ofs,
	    write_len, r);
    return r;
  }


  r = write_part_header(hctx, part_header);
  if (r < 0) {
    CLS_LOG(10, "%s(): failed to write header: r=%d", __func__, r);
    return r;
  }

  if (entries_pushed == 0) {
    CLS_LOG(0, "%s(): pushed no entries? Can't happen!", __func__);
    return -EFAULT;
  }

  return entries_pushed;
}

class EntryReader {
  static constexpr std::uint64_t prefetch_len = (128 * 1024);

  cls_method_context_t hctx;

  const fifo::part_header& part_header;

  std::uint64_t ofs;
  ceph::buffer::list data;

  int fetch(std::uint64_t num_bytes);
  int read(std::uint64_t num_bytes, ceph::buffer::list* pbl);
  int peek(std::uint64_t num_bytes, char *dest);
  int seek(std::uint64_t num_bytes);

public:
  EntryReader(cls_method_context_t hctx,
              const fifo::part_header& part_header,
              uint64_t ofs) : hctx(hctx),
			      part_header(part_header),
			      ofs(ofs < part_header.min_ofs ?
				  part_header.min_ofs :
				  ofs) {}

  std::uint64_t get_ofs() const {
    return ofs;
  }

  bool end() const {
    return (ofs >= part_header.next_ofs);
  }

  int peek_pre_header(entry_header_pre* pre_header);
  int get_next_entry(ceph::buffer::list* pbl,
                     std::uint64_t* pofs,
                     ceph::real_time* pmtime);
};


int EntryReader::fetch(std::uint64_t num_bytes)
{
  CLS_LOG(10, "%s(): fetch %d bytes, ofs=%d data.length()=%d", __func__, (int)num_bytes, (int)ofs, (int)data.length());
  if (data.length() < num_bytes) {
    ceph::buffer::list bl;
    CLS_LOG(10, "%s(): reading % " PRId64 " bytes at ofs=%" PRId64, __func__,
	    prefetch_len, ofs + data.length());
    int r = cls_cxx_read2(hctx, ofs + data.length(), prefetch_len, &bl, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
    if (r < 0) {
      CLS_ERR("ERROR: %s(): cls_cxx_read2() on obj returned %d", __func__, r);
      return r;
    }
    data.claim_append(bl);
  }

  if (static_cast<unsigned>(num_bytes) > data.length()) {
    CLS_LOG(10, "%s(): requested %" PRId64 " bytes, but only "
	    "%u were available", __func__, num_bytes, data.length());
    return -ERANGE;
  }

  return 0;
}

int EntryReader::read(std::uint64_t num_bytes, ceph::buffer::list* pbl)
{
  int r = fetch(num_bytes);
  if (r < 0) {
    return r;
  }
  data.splice(0, num_bytes, pbl);

  ofs += num_bytes;

  return 0;
}

int EntryReader::peek(std::uint64_t num_bytes, char* dest)
{
  int r = fetch(num_bytes);
  if (r < 0) {
    return r;
  }

  data.begin().copy(num_bytes, dest);

  return 0;
}

int EntryReader::seek(std::uint64_t num_bytes)
{
  ceph::buffer::list bl;

  CLS_LOG(10, "%s():%d: num_bytes=%" PRIu64, __func__, __LINE__, num_bytes);
  return read(num_bytes, &bl);
}

int EntryReader::peek_pre_header(entry_header_pre* pre_header)
{
  if (end()) {
    return -ENOENT;
  }

  int r = peek(sizeof(*pre_header),
	       reinterpret_cast<char*>(pre_header));
  if (r < 0) {
    CLS_ERR("ERROR: %s(): peek() size=%zu failed: r=%d", __func__,
	    sizeof(pre_header), r);
    return r;
  }

  if (pre_header->magic != part_header.magic) {
    CLS_ERR("ERROR: %s(): unexpected pre_header magic", __func__);
    return -ERANGE;
  }

  return 0;
}


int EntryReader::get_next_entry(ceph::buffer::list* pbl,
                                std::uint64_t* pofs,
                                ceph::real_time* pmtime)
{
  entry_header_pre pre_header;
  int r = peek_pre_header(&pre_header);
  if (r < 0) {
    CLS_ERR("ERROR: %s(): peek_pre_header() failed: r=%d", __func__, r);
    return r;
  }

  if (pofs) {
    *pofs = ofs;
  }

  CLS_LOG(10, "%s():%d: pre_header.pre_size=%llu", __func__, __LINE__,
	  pre_header.pre_size);
  r = seek(pre_header.pre_size);
  if (r < 0) {
    CLS_ERR("ERROR: %s(): failed to seek: r=%d", __func__, r);
    return r;
  }

  ceph::buffer::list header;
  CLS_LOG(10, "%s():%d: pre_header.header_size=%d", __func__, __LINE__, (int)pre_header.header_size);
  r = read(pre_header.header_size, &header);
  if (r < 0) {
    CLS_ERR("ERROR: %s(): failed to read entry header: r=%d", __func__, r);
    return r;
  }

  entry_header entry_header;
  auto iter = header.cbegin();
  try {
    decode(entry_header, iter);
  } catch (ceph::buffer::error& err) {
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

int trim_part(cls_method_context_t hctx,
	      ceph::buffer::list *in, ceph::buffer::list *out)
{
  CLS_LOG(10, "%s", __func__);

  op::trim_part op;
  try {
    auto iter = in->cbegin();
    decode(op, iter);
  } catch (const ceph::buffer::error &err) {
    CLS_ERR("ERROR: %s(): failed to decode request", __func__);
    return -EINVAL;
  }

  part_header part_header;
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
  if (op.exclusive && op.ofs == part_header.min_ofs) {
    return 0;
  }

  if (op.ofs >= part_header.next_ofs) {
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

    part_header.min_ofs = part_header.next_ofs;
    part_header.min_index = part_header.max_index;
  } else {
    EntryReader reader(hctx, part_header, op.ofs);

    entry_header_pre pre_header;
    int r = reader.peek_pre_header(&pre_header);
    if (r < 0) {
      return r;
    }

    if (op.exclusive) {
      part_header.min_index = pre_header.index;
    } else {
      r = reader.get_next_entry(nullptr, nullptr, nullptr);
      if (r < 0) {
	CLS_ERR("ERROR: %s(): unexpected failure at get_next_entry(): r=%d",
		__func__, r);
	return r;
      }
      part_header.min_index = pre_header.index + 1;
    }

    part_header.min_ofs = reader.get_ofs();
  }

  r = write_part_header(hctx, part_header);
  if (r < 0) {
    CLS_LOG(10, "%s(): failed to write header: r=%d", __func__, r);
    return r;
  }

  return 0;
}

int list_part(cls_method_context_t hctx, ceph::buffer::list* in,
	      ceph::buffer::list* out)
{
  CLS_LOG(10, "%s", __func__);

  op::list_part op;
  try {
    auto iter = in->cbegin();
    decode(op, iter);
  } catch (const buffer::error &err) {
    CLS_ERR("ERROR: %s(): failed to decode request", __func__);
    return -EINVAL;
  }

  part_header part_header;
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

  op::list_part_reply reply;

  reply.tag = part_header.tag;

  auto max_entries = std::min(op.max_entries, op::MAX_LIST_ENTRIES);

  for (int i = 0; i < max_entries && !reader.end(); ++i) {
    ceph::buffer::list data;
    ceph::real_time mtime;
    std::uint64_t ofs;

    r = reader.get_next_entry(&data, &ofs, &mtime);
    if (r < 0) {
      CLS_ERR("ERROR: %s(): unexpected failure at get_next_entry(): r=%d",
	      __func__, r);
      return r;
    }

    reply.entries.emplace_back(std::move(data), ofs, mtime);
  }

  reply.more = !reader.end();
  reply.full_part = full_part(part_header);

  encode(reply, *out);

  return 0;
}

int get_part_info(cls_method_context_t hctx, ceph::buffer::list *in,
		  ceph::buffer::list *out)
{
  CLS_LOG(10, "%s", __func__);

  op::get_part_info op;
  try {
    auto iter = in->cbegin();
    decode(op, iter);
  } catch (const ceph::buffer::error &err) {
    CLS_ERR("ERROR: %s(): failed to decode request", __func__);
    return -EINVAL;
  }

  op::get_part_info_reply reply;

  int r = read_part_header(hctx, &reply.header);
  if (r < 0) {
    CLS_LOG(10, "%s(): failed to read part header", __func__);
    return r;
  }

  encode(reply, *out);

  return 0;
}
}
} // namespace rados::cls::fifo

CLS_INIT(fifo)
{
  using namespace rados::cls::fifo;
  CLS_LOG(10, "Loaded fifo class!");

  cls_handle_t h_class;
  cls_method_handle_t h_create_meta;
  cls_method_handle_t h_get_meta;
  cls_method_handle_t h_update_meta;
  cls_method_handle_t h_init_part;
  cls_method_handle_t h_push_part;
  cls_method_handle_t h_trim_part;
  cls_method_handle_t h_list_part;
  cls_method_handle_t h_get_part_info;

  cls_register(op::CLASS, &h_class);
  cls_register_cxx_method(h_class, op::CREATE_META,
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          create_meta, &h_create_meta);

  cls_register_cxx_method(h_class, op::GET_META,
                          CLS_METHOD_RD,
                          get_meta, &h_get_meta);

  cls_register_cxx_method(h_class, op::UPDATE_META,
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          update_meta, &h_update_meta);

  cls_register_cxx_method(h_class, op::INIT_PART,
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          init_part, &h_init_part);

  cls_register_cxx_method(h_class, op::PUSH_PART,
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          push_part, &h_push_part);

  cls_register_cxx_method(h_class, op::TRIM_PART,
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          trim_part, &h_trim_part);

  cls_register_cxx_method(h_class, op::LIST_PART,
                          CLS_METHOD_RD,
                          list_part, &h_list_part);

  cls_register_cxx_method(h_class, op::GET_PART_INFO,
                          CLS_METHOD_RD,
                          get_part_info, &h_get_part_info);

  /* calculate entry overhead */
  struct entry_header entry_header;
  ceph::buffer::list entry_header_bl;
  encode(entry_header, entry_header_bl);

  part_entry_overhead = sizeof(entry_header_pre) + entry_header_bl.length();

  return;
}
