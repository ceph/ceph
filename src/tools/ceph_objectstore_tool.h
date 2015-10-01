// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_OBJECTSTORE_TOOL_H_
#define CEPH_OBJECTSTORE_TOOL_H_

#include "RadosDump.h"

class ObjectStoreTool : public RadosDump
{
  public:
    ObjectStoreTool(int file_fd, bool dry_run)
      : RadosDump(file_fd, dry_run)
    {}

    int do_import(ObjectStore *store, OSDSuperblock& sb, bool force,
		  std::string pgidstr,
		  ObjectStore::Sequencer &osr);
    int do_export(ObjectStore *fs, coll_t coll, spg_t pgid,
          pg_info_t &info, epoch_t map_epoch, __u8 struct_ver,
          const OSDSuperblock& superblock,
          map<epoch_t,pg_interval_t> &past_intervals);
    int get_object(ObjectStore *store, coll_t coll,
		   bufferlist &bl, OSDMap &curmap, bool *skipped_objects,
		   ObjectStore::Sequencer &osr);
    int export_file(
        ObjectStore *store, coll_t cid, ghobject_t &obj);
    int export_files(ObjectStore *store, coll_t coll);
};

#endif // CEPH_OBJECSTORE_TOOL_H_
