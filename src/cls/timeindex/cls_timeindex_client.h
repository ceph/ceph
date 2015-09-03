// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_TIMEINDEX_CLIENT_H
#define CEPH_CLS_TIMEINDEX_CLIENT_H

#include "include/types.h"
#include "include/rados/librados.hpp"
#include "cls_timeindex_types.h"

/*
 * timeindex objclass
 */

void cls_timeindex_add_prepare_entry(cls_timeindex_entry& entry,
                                     const utime_t& key_timestamp,
                                     const string& key_ext,
                                     bufferlist& bl);

void cls_timeindex_add(librados::ObjectWriteOperation& op,
                       const list<cls_timeindex_entry>& entry);

void cls_timeindex_add(librados::ObjectWriteOperation& op,
                       const cls_timeindex_entry& entry);

void cls_timeindex_add(librados::ObjectWriteOperation& op,
                       const utime_t& timestamp,
                       const string& name,
                       const bufferlist& bl);

void cls_timeindex_list(librados::ObjectReadOperation& op,
                        const utime_t& from,
                        const utime_t& to,
                        const string& in_marker,
                        const int max_entries,
                        list<cls_timeindex_entry>& entries,
                        string *out_marker,
                        bool *truncated);

void cls_timeindex_trim(librados::ObjectWriteOperation& op,
                        const utime_t& from_time,
                        const utime_t& to_time,
                        const string& from_marker = std::string(),
                        const string& to_marker   = std::string());

int cls_timeindex_trim(librados::IoCtx& io_ctx,
                       const string& oid,
                       const utime_t& from_time,
                       const utime_t& to_time,
                       const string& from_marker  = std::string(),
                       const string& to_marker    = std::string());
#endif
