// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 David Zafman <dzafman@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include <string>

#ifndef CEPH_LIBRADOS_LISTOBJECTIMPL_H
#define CEPH_LIBRADOS_LISTOBJECTIMPL_H

#include <include/rados/librados.hpp>

namespace librados {
struct ListObjectImpl {
  std::string nspace;
  std::string oid;
  std::string locator;

  ListObjectImpl() {}
  ListObjectImpl(std::string n, std::string o, std::string l):
      nspace(n), oid(o), locator(l) {}

  const std::string& get_nspace() const { return nspace; }
  const std::string& get_oid() const { return oid; }
  const std::string& get_locator() const { return locator; }
};
WRITE_EQ_OPERATORS_3(ListObjectImpl, nspace, oid, locator)
WRITE_CMP_OPERATORS_3(ListObjectImpl, nspace, oid, locator)
inline std::ostream& operator<<(std::ostream& out, const struct ListObjectImpl& lop) {
  out << (lop.nspace.size() ? lop.nspace + "/" : "") << lop.oid
      << (lop.locator.size() ? "@" + lop.locator : "");
  return out;
}

struct MRCObjectImpl {
  std::string nspace;
  int pgid;
  std::vector<int> mrc;

  MRCObjectImpl():pgid(-1) {}
  MRCObjectImpl(std::string n, int p, std::vector<int> m):
      nspace(n), pgid(p), mrc(m) {}

  const std::string& get_nspace() const { return nspace; }
  const int& get_pgid() const { return pgid; }
  const std::vector<int>& get_mrc() const { return mrc; }
};
WRITE_EQ_OPERATORS_3(MRCObjectImpl, nspace, pgid, mrc)
WRITE_CMP_OPERATORS_3(MRCObjectImpl, nspace, pgid, mrc)
inline std::ostream& operator<<(std::ostream& out, const struct MRCObjectImpl& lop) {
  out << (lop.nspace.size() ? lop.nspace + "/" : "") << lop.pgid;
  return out;
}

struct ObjListCtx;
struct MRCListCtx;

class NObjectIteratorImpl {
  public:
    NObjectIteratorImpl() {}
    ~NObjectIteratorImpl();
    NObjectIteratorImpl(const NObjectIteratorImpl &rhs);
    NObjectIteratorImpl& operator=(const NObjectIteratorImpl& rhs);

    bool operator==(const NObjectIteratorImpl& rhs) const;
    bool operator!=(const NObjectIteratorImpl& rhs) const;
    const ListObject& operator*() const;
    const ListObject* operator->() const;
    NObjectIteratorImpl &operator++(); // Preincrement
    NObjectIteratorImpl operator++(int); // Postincrement
    const ListObject *get_listobjectp() { return &cur_obj; }
    friend class IoCtx;
    friend struct ListObjectImpl;
    //friend class ListObject;
    friend class NObjectIterator;

    /// get current hash position of the iterator, rounded to the current pg
    uint32_t get_pg_hash_position() const;

    /// move the iterator to a given hash position.  this may (will!) be rounded to the nearest pg.
    uint32_t seek(uint32_t pos);

    void set_filter(const bufferlist &bl);

  private:
    NObjectIteratorImpl(ObjListCtx *ctx_);
    void get_next();
    ceph::shared_ptr < ObjListCtx > ctx;
    ListObject cur_obj;
};

class MRCIteratorImpl {
  public:
    MRCIteratorImpl() {}
    ~MRCIteratorImpl();
    MRCIteratorImpl(const MRCIteratorImpl &rhs);
    MRCIteratorImpl& operator=(const MRCIteratorImpl& rhs);

    bool operator==(const MRCIteratorImpl& rhs) const;
    bool operator!=(const MRCIteratorImpl& rhs) const;
    const MRCObject& operator*() const;
    const MRCObject* operator->() const;
    MRCIteratorImpl &operator++(); // Preincrement
    MRCIteratorImpl operator++(int); // Postincrement
    const MRCObject *get_MRCObjectp() { return &cur_obj; }
    friend class IoCtx;
    friend struct MRCObjectImpl;
    //friend class ListObject;
    friend class MRCIterator;


    /// move the iterator to a given hash position.  this may (will!) be rounded to the nearest pg.
    uint32_t seek(uint32_t pos);

  private:
    MRCIteratorImpl(MRCListCtx *ctx_);
    void get_next();
    ceph::shared_ptr < MRCListCtx > ctx;
    MRCObject cur_obj;
};

}
#endif
