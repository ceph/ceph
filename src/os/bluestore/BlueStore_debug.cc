// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include <cstdint>
#include <deque>
#include <ostream>
#include "BlueStore.h"
#include "Allocator.h"
#include "common/debug.h"
#include "include/random.h"

#define dout_subsys ceph_subsys_bluestore

static const std::string transition_table[26] = {
"bcdfghjklmnprstuvxyz", //a
"aeiloruy",//b
"aeiloruvy",//c
"aeilmnoruvy",//d
"bcdfghjklmnprstvxz",//e

"ailou",//f
"aeilnoru",//g
"aeiloru",//h
"dfghklmnpqrstvwx",//i
"aeiou",//j

"aeiloru",//k
"aeimnou",//l
"aeinotuy",//m
"aeiou",//n
"bcdfghjklmnpqrstvwxz",//o
"aehiloruy",//p

"aeiloru",//q
"adefiklmnotuvy",//r
"aehiklmnopqrtuvwy",//s
"acefhiklmnorsuvwy",//t
"bcdfghklmnpqrsvwxyz",//u

"acdeiklmorsu",//v
"aehilnorstu",//w
"aeilnorstuy",//x
"aehinorsuxz",//y
"aeiouy" //z
};

std::string int_to_fancy_name(uint64_t x)
{
  std::string result;
  uint8_t c = x % 26;
  x = x / 26;
  result.push_back(c+'a');
  while (x > 0) {
    uint8_t range = transition_table[c].length();
    uint8_t p = x % range;
    c = transition_table[c][p] - 'a';
    x = x / range;
    result.push_back(c+'a');
  }
  return result;
}

// Use special printing for multiplies of 1024; print K suffix
struct maybe_K {
  uint32_t x;
  maybe_K(uint32_t x) : x(x) {}
};
std::ostream &operator<<(std::ostream &out, const maybe_K &k) {
  if (((k.x & 0x3ff) == 0) && (k.x != 0)) {
    if (k.x != 0x400)
      out << (k.x / 0x400);
    out << "K";
  } else {
    out << std::hex << k.x << std::dec;
  }
  return out;
}
// cheap, not very reliable but portable detector where heap starts
static std::unique_ptr<char> heap_begin(new char);
std::ostream& operator<<(std::ostream& out, const BlueStore::Buffer& b);
std::ostream& operator<<(std::ostream& out, const BlueStore::Blob::printer &p)
{
  using P = BlueStore::printer;
  out << "Blob(";
  if (p.mode & P::PTR) {
    out << &p.blob;
  }
  if (p.mode & P::NICK) {
    uint64_t v = uint64_t(&p.blob);
    //Assume allocated Blobs will be 16 bytes aligned.
    v = (v - (uintptr_t)heap_begin.get()) / 16;
    out << int_to_fancy_name(v);
  }
  const bluestore_blob_t& bblob = p.blob.get_blob();
  if (p.mode & P::DISK) {
    //use default printer for std::vector * bluestore_pextent_t
    out << " disk=" << bblob.get_extents();
  }
  if (p.mode & P::SDISK) {
    const PExtentVector& ev = bblob.get_extents();
    uint64_t bits = 0;
    for (auto i : ev) {
      if (i.is_valid()) bits |= i.offset;
      bits |= i.length;
    }
    uint32_t zeros = 0; //zeros to apply to all values
    while ((bits & 0xf) == 0) {
      bits = bits >> 4;
      ++zeros;
    }
    out << " disk=0x[" << std::hex;
    for (size_t i = 0; i < ev.size(); ++i) {
      if (i != 0) {
        out << ",";
      }
      if (ev[i].is_valid()) {
        out << (ev[i].offset >> zeros * 4) << "~";
      } else {
        out << "!";
      }
      out << (ev[i].length >> zeros * 4);
    }
    out << "]" << std::dec;
    while (zeros > 0) {
      out << "0";
      --zeros;
    }
  }
  //always print lengths, if not printing use tracker
  if ((!(p.mode & (P::USE | P::SUSE)) || bblob.is_compressed())
    && !(p.mode & P::JUSTID)) {
    // Need to print blob logical length, no tracker printing
    // + there is no real tracker for compressed blobs
    if (bblob.is_compressed()) {
      out << " len=" << std::hex << bblob.get_logical_length() << "->"
          << bblob.get_compressed_payload_length() << std::dec;
    } else {
      out << " len=" << std::hex << bblob.get_logical_length() << std::dec;
    }
  }
  if ((p.mode & P::USE) && !bblob.is_compressed()) {
    out << " " << p.blob.get_blob_use_tracker();
  }
  if (p.mode & P::SUSE) {
    auto& tracker = p.blob.get_blob_use_tracker();
    if (bblob.is_compressed()) {
      out << " [" << std::hex << tracker.get_referenced_bytes() << std::dec << "]";
    } else {
      const uint32_t* au_array = tracker.get_au_array();
      uint16_t zeros = 0;
      uint16_t full = 0;
      uint16_t num_au = tracker.get_num_au();
      uint32_t au_size = tracker.au_size;
      uint32_t def = std::numeric_limits<uint32_t>::max();
      out << " track=" << tracker.get_num_au() << "*" << maybe_K(tracker.au_size);
      for (size_t i = 0; i < num_au; i++) {
        if (au_array[i] == 0) ++zeros;
        if (au_array[i] == au_size) ++full;
      }
      if (zeros >= num_au - 3 && num_au > 6) def = 0;
      if (full >= num_au - 3 && num_au > 6) def = au_size;
      if (def != std::numeric_limits<uint32_t>::max()) {
        out << " {" << maybe_K(def) << "}[";
        for (size_t i = 0; i < num_au; i++) {
          if (au_array[i] != def) {
            out << i << "=" << maybe_K(au_array[i]);
            ++i;
            for (; i < num_au; i++) {
              if (au_array[i] != def) {
                out << "," << i << "=" << maybe_K(au_array[i]);
              }
            }
          }
        }
        out << "]";
      } else {
        out << " [";
        for (size_t i = 0; i < num_au; i++) {
          if (i != 0) out << ",";
          out << maybe_K(au_array[i]);
        }
        out << "]";
      }
    }
  }
  if (bblob.has_csum()) {
    if (p.mode & (P::SCHK | P::CHK)) {
      out << " " << Checksummer::get_csum_type_string(bblob.csum_type) << "/"
          << (int)bblob.csum_chunk_order << "/" << bblob.csum_data.length();
    }
    if (p.mode & P::CHK) {
      std::vector<uint64_t> v;
      unsigned n = bblob.get_csum_count();
      for (unsigned i = 0; i < n; ++i)
        v.push_back(bblob.get_csum_item(i));
      out << " " << std::hex << v << std::dec;
    }
  }
  if (!(p.mode & P::JUSTID) && p.blob.is_spanning()) {
    out << " spanning.id=" << p.blob.id;
  }
  if (!(p.mode & P::JUSTID) &&
    p.blob.shared_blob &&
    (p.blob.shared_blob->get_sbid() != 0)) {
    out << " " << *p.blob.shared_blob;
  }
  out << ")";
  return out;
}

std::ostream& operator<<(std::ostream& out, const BlueStore::Extent::printer &p)
{
  out << std::hex << "0x" << p.ext.logical_offset << "~" << p.ext.length
    << ": 0x" << p.ext.blob_offset << "~" << p.ext.length << std::dec
	<< " " << p.ext.blob->print(p.mode);
  return out;
}

std::ostream& operator<<(std::ostream& out, const BlueStore::Onode::printer &p)
{
  using P = BlueStore::printer;
  const BlueStore::Onode& o = p.onode;
  uint16_t mode = p.mode;
  out << &o << " " << o.oid
      << " nid " << o.onode.nid
      << " size 0x" << std::hex << o.onode.size
      << " (" << std::dec << o.onode.size << ")"
      << " expected_object_size " << o.onode.expected_object_size
      << " expected_write_size " << o.onode.expected_write_size
      << " in " << o.onode.extent_map_shards.size() << " shards";
  if (o.onode.extent_map_shards.size() > 0) {
    out << ":" << std::hex;
    for (auto& s : o.onode.extent_map_shards) {
      out << " 0x" << s.offset;
    }
    out << std::dec;
  }
  out << ", " << o.extent_map.spanning_blob_map.size()
      << " spanning blobs";
  const BlueStore::ExtentMap& map = o.extent_map;
  std::set<BlueStore::Blob*> visited;
  // to make printing extents in-sync with blobs
  uint16_t mode_extent = (mode & (P::PTR | P::NICK)) | P::JUSTID;
  auto i = map.seek_lextent(p.from);
  while (i != map.extent_map.end() && i->logical_offset < p.end) {
    BlueStore::Blob* b = i->blob.get();
    out << std::endl << i->print(mode_extent);
    if (!visited.contains(b)) {
      visited.insert(b);
    }
    ++i;
  }
  for (const auto& i : visited) {
    out << std::endl << i->print(mode);
  }
  // here printing Buffers
  if (p.mode & (P::BUF | P::SBUF)) {
    std::lock_guard l(o.c->cache->lock);
    if (p.mode & P::SBUF) {
      // summary buf mode, only print what is mapped what options are, one liner
      out << " bufs(";
      bool space = false;
      for (auto& i : o.bc.buffer_map) {
        if (space) out << " ";
        out << "0x" << std::hex << i.offset << "~" << i.length << std::dec
          << BlueStore::Buffer::get_state_name_short(i.state);
        if (i.flags) {
          out << "," << BlueStore::Buffer::get_flag_name(i.flags);
        }
        space = true;
      }
      out << ")";
    } else {
      for (auto& i : o.bc.buffer_map) {
        out << std::endl << "  0x" << std::hex << i.offset
          << "~" << i.length << std::dec
          << " " << i;
      }
    }
  }
  if (mode & P::ATTRS) {
    for (const auto& p : o.onode.attrs) {
      out << std::endl << "attr " << p.first << " len " << p.second.length();
    }
  }
  return out;
}

// =======================================================

// ClaimRangeStressThread

#undef dout_prefix
#define dout_prefix *_dout << "bluestore.ClaimRangeStressThread "
#undef dout_context
#define dout_context store->cct

void BlueStore::ClaimRangeStressThread::init()
{
  auto& conf = store->cct->_conf;
  if (!conf.get_val<bool>("bluestore_debug_claim_range_stress")) {
    return;
  }
  max_claims = conf.get_val<uint64_t>(
    "bluestore_debug_claim_range_stress_max_claims");
  claim_len = conf.get_val<Option::size_t>(
    "bluestore_debug_claim_range_stress_claim_length");
  free_p = conf.get_val<uint64_t>(
    "bluestore_debug_claim_range_stress_max_free_percent");
  min_free = conf.get_val<Option::size_t>(
    "bluestore_debug_claim_range_stress_min_free");
  interval = conf.get_val<double>(
    "bluestore_debug_claim_range_stress_interval");
  sentinel_samples = conf.get_val<uint64_t>(
    "bluestore_debug_claim_range_stress_sentinel_samples");
  write_sentinel = conf.get_val<bool>(
    "bluestore_debug_claim_range_stress_write_sentinel");
  // claim_range() asserts on a misaligned length;
  ceph_assert(claim_len % store->alloc->get_block_size() == 0);
  ceph_assert(claim_len <= uint64_t(store->alloc->get_capacity()));
  ceph_assert(!stop);
  create("bstore_claim_st");
}

void *BlueStore::ClaimRangeStressThread::entry()
{
  std::unique_lock l{lock};

  struct held_claim {
    PExtentVector exts;
    uint64_t bytes;
  };
  std::deque<held_claim> pool;
  uint64_t held = 0, claimed_total = 0, released_total = 0;
  // claim_range() calls, by how much of the range was free
  uint64_t claims_full = 0, claims_partial = 0, claims_zero = 0;
  // sentinel blocks written at the edges and in the middle of extents
  uint64_t sentinels_edge = 0, sentinels_middle = 0;
  const uint64_t bs = store->block_size;
  bufferlist sentinel;
  sentinel.append(std::string(bs, char(0xAB)));

  auto release_front = [&] {
    auto &claimed_range = pool.front();
    store->alloc->release(claimed_range.exts);
    released_total += claimed_range.bytes;
    held -= claimed_range.bytes;
    pool.pop_front();
  };

  while (!stop) {
    if (pool.size() == max_claims) {
      release_front();
    }
    uint64_t free = store->alloc->get_free();

    if (held + claim_len <= free_p * (held + free) / 100 &&
        free >= min_free + claim_len) {
      // start at any allocation unit, so claims straddle the edges of held
      // claims and of the workload's extents; the last start that fits keeps
      // [off, off + claim_len) within the allocator's capacity
      uint64_t au = store->alloc->get_block_size();
      uint64_t last = (store->alloc->get_capacity() - claim_len) / au;
      uint64_t off =
        ceph::util::generate_random_number<uint64_t>(0, last) * au;
      PExtentVector exts;
      uint64_t n = store->alloc->claim_range(off, claim_len, &exts);
      dout(20) << __func__ << " claimed 0x" << std::hex << n << " of 0x"
               << off << "~" << claim_len << std::dec << dendl;
      if (n == claim_len) {
        ++claims_full;
      } else if (n > 0) {
        ++claims_partial;
      } else {
        ++claims_zero;
      }
      if (n > 0) {
        if (write_sentinel) {
          // write sentinel to the first and the last block of the extent,
          // and to sentinel_samples random blocks in between
          for (auto& e : exts) {
            int r = store->bdev->write(e.offset, sentinel, false);
            ceph_assert(r == 0);
            r = store->bdev->write(e.offset + e.length - bs, sentinel, false);
            ceph_assert(r == 0);
            uint64_t blocks = e.length / bs;
            // a one-block extent got the same block twice
            sentinels_edge += blocks > 1 ? 2 : 1;
            if (blocks > 2) {
              for (uint64_t i = 0; i < sentinel_samples; ++i) {
                uint64_t b =
                  ceph::util::generate_random_number<uint64_t>(1, blocks - 2);
                r = store->bdev->write(e.offset + b * bs, sentinel, false);
                ceph_assert(r == 0);
                ++sentinels_middle;
              }
            }
          }
        }
        claimed_total += n;
        held += n;
        pool.push_back({std::move(exts), n});
      }
    }
    cond.wait_for(l, ceph::make_timespan(interval));
  }

  // Thread stopped, release all claims
  while (!pool.empty()) {
    release_front();
  }

  dout(1) << __func__ << " claims "
          << claims_full + claims_partial + claims_zero
          << " (full " << claims_full
          << " partial " << claims_partial
          << " zero " << claims_zero << ")"
          << " sentinels edge " << sentinels_edge
          << " middle " << sentinels_middle
          << " claimed " << byte_u_t(claimed_total)
          << " released " << byte_u_t(released_total) << dendl;
  ceph_assert(held == 0);
  ceph_assert(claimed_total == released_total);
  stop = false;
  return nullptr;
}
