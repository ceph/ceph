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

#ifndef CEPH_OSD_OBJECTVERSIONER_H
#define CEPH_OSD_OBJECTVERSIONER_H

class ObjectVersioner {
 public:
  pobject_t oid;

  void get_versions(list<version_t>& ls);
  version_t head();      // newest
  version_t committed(); // last committed
  version_t tail();      // oldest

  /* 
   * prepare a new version, starting wit "raw" transaction t.
   */
  void prepare(ObjectStore::Transaction& t, version_t v);
  void rollback_to(version_t v);
  void commit_to(version_t v);
};

#endif
