/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef RGW_SNAPSHOT_H_
#define RGW_SNAPSHOT_H_

#include <string>
#include <memory>

#include "include/types.h"
#include "include/utime.h"
#include "rgw_common.h"
#include "rgw_tools.h"

#include "rgw_rados.h"

#include "rgw_string.h"

#include "common/Formatter.h"
#include "common/lru_map.h"
#include "rgw_formats.h"


class RGWRados;
class CephContext;

using namespace std;

class RGWSnapshot {
protected:
  CephContext *cct;
  RGWRados *store;
  
  static
  int get_rados_pools( CephContext *cct, RGWRados *store, list<string>& pools);

public:
  RGWSnapshot( CephContext *_cct, RGWRados *_store, const string& _snap_name);

  static
  int get_snapshots( CephContext *cct, RGWRados *store, 
                     list<string> pools, list<RGWSnapshot>& snaps);
  static
  int get_rgw_pools( CephContext *cct, RGWRados *store, list<string>& pools);

  int snap_num;
  string snap_name;
  time_t snap_created;

  int create();
  int remove();

  void dump(Formatter *f) const;
};

#endif /* RGW_SNAPSHOT_H */
