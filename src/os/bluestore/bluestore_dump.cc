// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>

#include <stdio.h>
#include <string.h>
#include <iostream>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "common/safe_io.h"

#include "os/bluestore/BlueFS.h"
#include "os/bluestore/BlueStore.h"
#include "common/admin_socket.h"
#include "common/url_escape.h"
namespace po = boost::program_options;

#include "bluestore_dump.h"

void Dump::dump(const bluestore_blob_t& t)
{
  f->open_array_section("extents");
  for(auto& e: t.get_extents()) {
    f->open_object_section("extent");
    if ((int64_t)e.offset != -1)
      f->dump_format_unquoted("o", "0x%x", e.offset);
    else
      f->dump_int("o", -1);
    f->dump_format_unquoted("l", "0x%x", e.length);
    f->close_section();
  }
  f->close_section();
  f->dump_format_unquoted("flags", "%d(%s)", t.flags, bluestore_blob_t::get_flags_string(t.flags).c_str());
  if (t.is_compressed()) {
    f->dump_unsigned("logical_length", t.get_logical_length());
    f->dump_unsigned("compressed_length", t.get_compressed_payload_length());
  }
  if (t.has_csum()) {
    f->open_object_section("csum");
    f->dump_format_unquoted("type", "%d(%s)", t.csum_type, Checksummer::get_csum_type_string(t.csum_type));
    f->dump_int("order", t.csum_chunk_order);
    char str[t.csum_data.length()*2+1];
    for (size_t i = 0; i < t.csum_data.length(); i++)
      sprintf(&str[i*2], "%2.2x", *(unsigned char*)(t.csum_data.c_str()+i));
    f->dump_format_unquoted("data", "%s", str);
    f->close_section();
  }
  if (t.has_unused()) {
    f->dump_unsigned("unused", t.unused);
  }
}


void Dump::dump(bluestore_onode_t& t)
{
  f->dump_unsigned("nid", t.nid);
  f->dump_unsigned("size", t.size);
  f->open_object_section("attrs");
  for (auto p = t.attrs.begin(); p != t.attrs.end(); ++p) {
    f->open_object_section("attr");
    f->dump_string("name", p->first.c_str());  // it's not quite std::string
    f->dump_string("val", url_escape(std::string(p->second.c_str(), p->second.length())));
    f->close_section();
  }
  f->close_section();
  f->dump_format_unquoted("flags", "%d(%s)", t.flags, t.get_flags_string().c_str());
  f->open_array_section("extent_map_shards");
  for(auto& e: t.extent_map_shards) {
    f->open_object_section("shard");
    f->dump_format_unquoted("offset", "0x%x", e.offset);
    f->dump_unsigned("bytes", e.bytes);
    f->close_section();
  }
  f->close_section();
  f->dump_unsigned("expected_object_size", t.expected_object_size);
  f->dump_unsigned("expected_write_size", t.expected_write_size);
  f->dump_unsigned("alloc_hint_flags", t.alloc_hint_flags);
}

void Dump::dump(const bluestore_blob_use_tracker_t& t)
{
  f->dump_int("au_size", t.au_size);
  if (t.au_size) {
    f->dump_int("num_au", t.num_au);
    if (!t.num_au) {
      f->dump_int("total_bytes", t.total_bytes);
    } else {
      f->open_array_section("bytes_per_au");
      for (size_t i = 0; i < t.num_au; ++i) {
	f->dump_int("bytes", t.bytes_per_au[i]);
      }
      f->close_section();
    }
  }
}


//equivalent to BlueStore::ExtentMap::decode_spanning_blobs
int Dump::decode_spanning_blobs(std::map<int16_t, bluestore_blob_t>& spanning_blobs,
				bufferptr::const_iterator& p)
{
  __u8 struct_v;
  denc(struct_v, p);
  f->dump_int("v", struct_v);
  ceph_assert(struct_v == 1 || struct_v == 2); //AKAK - cannot be asserts!

  unsigned n;
  denc_varint(n, p);
  f->dump_int("blobs_cnt", n);

  f->open_array_section("blobs");
  while (n--) {
    decltype(BlueStore::Blob::id) blob_id; //int16_t
    denc_varint(blob_id, p);
    f->open_object_section("a_blob");
    f->dump_int("blob_id", blob_id);

    bluestore_blob_t bluestore_blob;
    denc(bluestore_blob, p, struct_v);
    spanning_blobs[blob_id] = bluestore_blob;

    f->open_object_section("bluestore_blob");
    dump(bluestore_blob);
    f->close_section();

    uint64_t shared_blob_id = 0;
    if (bluestore_blob.is_shared()) {
      denc(shared_blob_id, p);
      f->dump_int("shared_blob_id", shared_blob_id);
      //AK maybe? ceph_assert(shared_blob_id != 0);
    }
    bluestore_blob_use_tracker_t bluestore_blob_use_tracker;
    if (struct_v > 1) {
      bluestore_blob_use_tracker.decode(p);
      f->open_object_section("blob_tracker");
      dump(bluestore_blob_use_tracker);
      f->close_section();
    } else {
      //legacy AKAK - TODO - not converted
      bluestore_extent_ref_map_t legacy_ref_map;
      legacy_ref_map.decode(p);
      f->open_object_section("legacy_ref_map");
      legacy_ref_map.dump(f);
      f->close_section();
    }
    f->close_section();//a_blob
  }
  f->close_section();//blobs
  return 0;
}

#define BLOBID_FLAG_CONTIGUOUS 0x1  // this extent starts at end of previous
#define BLOBID_FLAG_ZEROOFFSET 0x2  // blob_offset is 0
#define BLOBID_FLAG_SAMELENGTH 0x4  // length matches previous extent
#define BLOBID_FLAG_SPANNING   0x8  // has spanning blob id
#define BLOBID_SHIFT_BITS        4

std::string Dump::blob_flags_to_string(uint8_t flags)
{
  stringstream o;
  if (flags & BLOBID_FLAG_CONTIGUOUS)
  {
    o << "cont";
    if (flags & ~BLOBID_FLAG_CONTIGUOUS)
      o << "+";
  }
  if (flags & BLOBID_FLAG_ZEROOFFSET)
  {
    o << "zeroofs";
    if (flags & ~(BLOBID_FLAG_CONTIGUOUS|BLOBID_FLAG_ZEROOFFSET))
      o << "+";
  }
  if (flags & BLOBID_FLAG_SAMELENGTH)
  {
    o << "samelen";
    if (flags & ~(BLOBID_FLAG_CONTIGUOUS|BLOBID_FLAG_ZEROOFFSET|BLOBID_FLAG_SAMELENGTH))
      o << "+";
  }
  if (flags & BLOBID_FLAG_SPANNING)
  {
    o << "spanning";
  }
  return o.str();
}


unsigned Dump::decode_some(const bufferlist& bl)
{

  ceph_assert(bl.get_num_buffers() <= 1);
  auto p = bl.front().begin_deep();
  __u8 struct_v;
  denc(struct_v, p);
  f->dump_int("v", struct_v);

  // Version 2 differs from v1 in blob's ref_map
  // serialization only. Hence there is no specific
  // handling at ExtentMap level below.
  ceph_assert(struct_v == 1 || struct_v == 2); //AKAK - cannot be asserts!

  uint32_t num;
  denc_varint(num, p);
  f->dump_int("extents_cnt", num);
  uint64_t pos = 0;
  uint64_t prev_len = 0;
  unsigned n = 0;

  bluestore_blob_t blob;

  f->open_array_section("extents");
  while (!p.end()) {
    f->open_object_section("extent");
    BlueStore::Extent le;
    uint64_t blobid;
    denc_varint(blobid, p);
    uint8_t flags = blobid & ((1 << BLOBID_SHIFT_BITS) - 1);
    f->dump_format_unquoted("flags", "%d(%s)", flags, blob_flags_to_string(flags).c_str());
    f->dump_int("blob_id", blobid >> BLOBID_SHIFT_BITS);
    if ((blobid & BLOBID_FLAG_CONTIGUOUS) == 0) {
      uint64_t gap;
      denc_varint_lowz(gap, p);
      f->dump_int("gap", gap);
      pos += gap;
    }
    le.logical_offset = pos;
    if ((blobid & BLOBID_FLAG_ZEROOFFSET) == 0) {
      denc_varint_lowz(le.blob_offset, p);
      f->dump_int("offset", le.blob_offset);
    } else {
      le.blob_offset = 0;
    }
    if ((blobid & BLOBID_FLAG_SAMELENGTH) == 0) {
      denc_varint_lowz(prev_len, p);
      f->dump_int("length", prev_len);
    }
    le.length = prev_len;
    //std::string blob_source;
    if (blobid & BLOBID_FLAG_SPANNING) {
      uint64_t defined_blob_id = blobid >> BLOBID_SHIFT_BITS;

      //AK using blobs defined as spanning blobs
      //blob_source = "spanning:" + to_string(defined_blob_id);
      f->dump_int("spanning_blob", defined_blob_id);
    } else {
      blobid >>= BLOBID_SHIFT_BITS;
      if (blobid) {
	//AK will use locally defined blobs
	//AK here we assume this (blobid - 1) is already created
	f->dump_format_unquoted("__uses_local_blob","(%d)", blobid - 1);
      } else {
	//AK blob defined right here, blob_id provided
	uint64_t shared_blob_id;
	denc(blob, p, struct_v);
	f->open_object_section("blob");
	f->dump_format_unquoted("__defines_local_blob","(%d)", n);
	dump(blob);
	if (blob.is_shared()) {
	  //AK it uses shared_blob. I wonder if we double check it somehow
	  denc(shared_blob_id, p);
	  f->dump_int("shared_blob_id", shared_blob_id);
	}
	f->close_section();
      }
      // we build ref_map dynamically for non-spanning blobs
      ///cout << blob_source << " " << to_string(blob) << " #" << to_string(le) << std::endl;
    }
    pos += prev_len;
    ++n;
    f->close_section(); //extent
  }
  f->close_section(); //extents
  ceph_assert(n == num);
  return num;
}


bool Dump::dump_object(const bufferlist& v)
{
  std::map<int16_t, bluestore_blob_t> spanning_blobs;
  size_t v_size = v.length();
  auto p = v.front().begin_deep();

  bluestore_onode_t bo;
  bo.decode(p);
  f->open_object_section("object");
  f->open_object_section("bluestore_onode_t");
  dump(bo);
  f->close_section();

  f->open_object_section("spanning_blobs");
  decode_spanning_blobs(spanning_blobs, p);
  f->close_section();

  if(bo.extent_map_shards.empty()) {
    bufferlist inline_bl;
    denc(inline_bl, p);
    f->open_object_section("extents");
    decode_some(inline_bl);
    f->close_section();
  } else {
    f->open_array_section("extent_map_shards");
    for (auto& x: bo.extent_map_shards) {
      f->open_object_section("extent");
      f->dump_format_unquoted("offset", "0x%x", x.offset);
      f->dump_int("bytes", x.bytes);
      f->close_section();
    }
    f->close_section();
  }
  f->close_section(); //Onode

  //TODO - improve on error with decoding size
  ceph_assert(p.get_offset() == v_size);
  return true;
}


bool Dump::dump_object(const std::string& key, const bufferlist& v)
{
  ghobject_t oid;
  int r = BlueStore::objkey_to_oid(key, &oid);
  if (r != 0) {
    cout << "failed to decode '" + url_escape(key) + "' error code=" + std::to_string(r) << std::endl;
    return false;
  }
  f->open_object_section("object");
  f->open_object_section("id");
  f->dump_string("hobj", oid.hobj.to_str());
  f->dump_int("shard", oid.shard_id);
  if (oid.generation != ghobject_t::NO_GEN)
    f->dump_unsigned("gen", oid.generation);
  else
    f->dump_int("gen", -1);
  f->close_section();
  dump_object(v);
  f->close_section();
  return true;
}


bool Dump::dump_extent(const std::string& key, const bufferlist& v)
{
  ghobject_t oid;
  uint32_t offset;
  int r = BlueStore::extentkey_to_oid(key, &oid, &offset);
  if (r != 0) {
    cerr << "failed to decode '" + url_escape(key) + "' error code=" + std::to_string(r) << std::endl;
    return false;
  }
  f->open_object_section("extent");
  f->open_object_section("id");
  f->dump_string("hobj", oid.hobj.to_str());
  f->dump_int("shard", oid.shard_id);
  if (oid.generation != ghobject_t::NO_GEN)
    f->dump_unsigned("gen", oid.generation);
  else
    f->dump_int("gen", -1);
  f->close_section();
  f->dump_unsigned("offset", offset);

  decode_some(v);
  f->close_section();
  return true;
}


int Dump::list_objects(const std::string& format,
		       const std::string& selector,
		       const std::string& trimmer) {
  KeyValueDB::Iterator obj_it = db->get_iterator("O");
  obj_it->seek_to_first();
  shared_ptr<Formatter> actual_formatter(Formatter::create(format));
  if (actual_formatter.get() == nullptr) {
    std::cout << "Invalid format '" << format << "'" << std::endl;
    return -1;
  }
  shared_ptr<Filter> ff;
  shared_ptr<Buffer> bf;
  shared_ptr<Trimmer> tf;
  Formatter* buffer;
  bool matched;

  if (!selector.empty()) {
    std::string error;
    ff.reset(Filter::create());
    bool b = ff->setup(selector, error);
    if (!b) {
      std::cout << error << std::endl;
      return -1;
    }
    bf.reset(Buffer::create());
  }

  if (!trimmer.empty()) {
    std::string error;
    tf.reset(Trimmer::create());
    bool b = tf->setup(trimmer, error);
    if (!b) {
      std::cout << error << std::endl;
      return -1;
    }
  }

  for(; obj_it->valid(); obj_it->next()) {
    f = &*actual_formatter;
    if (!trimmer.empty()) {
      f = tf->attach(f);
    }
    if (!selector.empty()) {
      matched = false;
      buffer = bf->attach(f);
      f = ff->attach(buffer, [&](){matched = true;});
    }
    std::string key = obj_it->key();
    ghobject_t oid;
    bool is_oid = *key.rbegin() == 'o';
    if (is_oid)
      dump_object(key, obj_it->value());
    else
      dump_extent(key, obj_it->value());
    if (!selector.empty()) {
      if (matched) {
	bf->unbuffer();
	actual_formatter->flush(cout);
      }
    } else {
      actual_formatter->flush(cout);
    }
  }
  return 0;
}
