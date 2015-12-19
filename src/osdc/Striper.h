// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_STRIPER_H
#define CEPH_STRIPER_H

#include "include/types.h"
#include "osd/osd_types.h"

class CephContext;

//namespace ceph {

  class Striper {
  public:
    /*
     * map (ino, layout, offset, len) to a (list of) ObjectExtents (byte
     * ranges in objects on (primary) osds)
     */
    static void file_to_extents(CephContext *cct, const char *object_format,
				const ceph_file_layout *layout,
				uint64_t offset, uint64_t len,
				uint64_t trunc_size,
				map<object_t, vector<ObjectExtent> >& extents,
				uint64_t buffer_offset=0);

    static void file_to_extents(CephContext *cct, const char *object_format,
				const ceph_file_layout *layout,
				uint64_t offset, uint64_t len,
				uint64_t trunc_size,
				vector<ObjectExtent>& extents,
				uint64_t buffer_offset=0);

    static void file_to_extents(CephContext *cct, inodeno_t ino,
				const ceph_file_layout *layout,
				uint64_t offset, uint64_t len,
				uint64_t trunc_size,
				vector<ObjectExtent>& extents) {
      // generate prefix/format
      char buf[32];
      snprintf(buf, sizeof(buf), "%llx.%%08llx", (long long unsigned)ino);

      file_to_extents(cct, buf, layout, offset, len, trunc_size, extents);
    }

    static void assimilate_extents(
      map<object_t, vector<ObjectExtent> >& object_extents,
      vector<ObjectExtent>& extents);

    /**
     * reverse map an object extent to file extents
     */
    static void extent_to_file(CephContext *cct, ceph_file_layout *layout,
			       uint64_t objectno, uint64_t off, uint64_t len,
			       vector<pair<uint64_t, uint64_t> >& extents);

    static uint64_t object_truncate_size(
      CephContext *cct, const ceph_file_layout *layout,
      uint64_t objectno, uint64_t trunc_size);

    static uint64_t get_num_objects(const ceph_file_layout& layout,
				    uint64_t size);
    /*
     * helper to assemble a striped result
     */
    class StripedReadResult {
      // offset -> (data, intended length)
      map<uint64_t, pair<bufferlist, uint64_t> > partial;

    public:
      void add_partial_result(
	CephContext *cct, bufferlist& bl,
	const vector<pair<uint64_t,uint64_t> >& buffer_extents);
      /**
       * add sparse read into results
       *
       * @param bl buffer
       * @param bl_map map of which logical source extents this covers
       * @param bl_off logical buffer offset (e.g., first bl_map key
       *               if the buffer is not sparse)
       * @param buffer_extents output buffer extents the data maps to
       */
      void add_partial_sparse_result(
	CephContext *cct, bufferlist& bl,
	const map<uint64_t, uint64_t>& bl_map, uint64_t bl_off,
	const vector<pair<uint64_t,uint64_t> >& buffer_extents);

      void assemble_result(CephContext *cct, bufferlist& bl, bool zero_tail);
    };

  };

//};

#endif
