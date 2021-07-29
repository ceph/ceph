// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2021 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#ifndef CEPH_OSD_MEMSTORE_OBJECT_H
#define CEPH_OSD_MEMSTORE_OBJECT_H

#include "common/RefCountedObj.h"
#include "os/ObjectStore.h"
#include "PageSet.h"

struct MemObject : public RefCountedObject {
  ceph::mutex xattr_mutex{ceph::make_mutex("MemObject::xattr_mutex")};
  ceph::mutex omap_mutex{ceph::make_mutex("MemObject::omap_mutex")};
  std::map<std::string,ceph::buffer::ptr,std::less<>> xattr;
  ceph::buffer::list omap_header;
  std::map<std::string,ceph::buffer::list> omap;

  using Ref = ceph::ref_t<MemObject>;

  // interface for object data
  virtual size_t get_size() const = 0;
  virtual int read(uint64_t offset, uint64_t len, ceph::buffer::list &bl) = 0;
  virtual int write(uint64_t offset, const ceph::buffer::list &bl) = 0;
  virtual int clone_range(MemObject *src, uint64_t srcoff, uint64_t len,
                    uint64_t dstoff) = 0;
  virtual int truncate(uint64_t offset) = 0;
  virtual void encode(ceph::buffer::list& bl) const = 0;
  virtual void decode(ceph::buffer::list::const_iterator& p) = 0;

  // xattrs and omap
  virtual int getattr(const char *name, ceph::buffer::ptr& value);
  virtual int getattrs(std::map<std::string,ceph::buffer::ptr,std::less<>>& aset);
  virtual int setattrs(std::map<string, ceph::buffer::ptr>& aset);
  virtual int rmattr(const char *name);
  virtual int rmattrs();
  virtual int omap_get(ceph::buffer::list *header,
                       std::map<std::string, ceph::buffer::list> *out);
  virtual int omap_get_header(ceph::buffer::list *header);
  virtual int omap_get_keys(std::set<std::string> *keys);
  virtual int omap_get_values(const std::set<std::string> &keys,
                              std::map<std::string, ceph::buffer::list> *out);
  virtual int omap_check_keys(const std::set<std::string> &keys,
                              std::set<std::string> *out);
  virtual int omap_clear();
  virtual int omap_setkeys(ceph::buffer::list& aset_bl);
  virtual int omap_rmkeys(ceph::buffer::list& keys_bl);
  virtual int omap_rmkeyrange(const std::string& first, const std::string& last);
  virtual int omap_setheader(const ceph::buffer::list &bl);

  int clone(MemObject* src);
  void encode_base(ceph::buffer::list& bl) const; 
  void decode_base(ceph::buffer::list::const_iterator& p);
  void dump(ceph::Formatter *f) const;

  class OmapIteratorImpl : public ObjectMap::ObjectMapIteratorImpl {
    MemObject& o;
    std::map<std::string,ceph::buffer::list>::iterator it;
  public:
    OmapIteratorImpl(MemObject& o)
      : o(o), it(o.omap.begin()) {}

    int seek_to_first() override {
      std::lock_guard lock{o.omap_mutex};
      it = o.omap.begin();
      return 0;
    }
    int upper_bound(const std::string &after) override {
      std::lock_guard lock{o.omap_mutex};
      it = o.omap.upper_bound(after);
      return 0;
    }
    int lower_bound(const std::string &to) override {
      std::lock_guard lock{o.omap_mutex};
      it = o.omap.lower_bound(to);
      return 0;
    }
    bool valid() override {
      std::lock_guard lock{o.omap_mutex};
      return it != o.omap.end();
    }
    int next() override {
      std::lock_guard lock{o.omap_mutex};
      ++it;
      return 0;
    }
    std::string key() override {
      std::lock_guard lock{o.omap_mutex};
      return it->first;
    }
    ceph::buffer::list value() override {
      std::lock_guard lock{o.omap_mutex};
      return it->second;
    }
    int status() override {
      return 0;
    }
  };
  ObjectMap::ObjectMapIterator get_omap_iterator() {
    return ObjectMap::ObjectMapIterator(new OmapIteratorImpl(*this));
  }
};

struct BufferlistObject : public MemObject {
  ceph::spinlock mutex;
  ceph::buffer::list data;

  size_t get_size() const override { return data.length(); }

  int read(uint64_t offset, uint64_t len, ceph::buffer::list &bl) override;
  int write(uint64_t offset, const ceph::buffer::list &bl) override;
  int clone_range(MemObject *src, uint64_t srcoff, uint64_t len,
                  uint64_t dstoff) override;
  int truncate(uint64_t offset) override;

  void encode(ceph::buffer::list& bl) const override {
    ENCODE_START(1, 1, bl);
    encode(data, bl);
    encode_base(bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& p) override {
    DECODE_START(1, p);
    decode(data, p);
    decode_base(p);
    DECODE_FINISH(p);
  }
};

struct VectorObject : public MemObject {
  ceph::spinlock mutex;
  std::vector<char> data;

  size_t get_size() const override { return data.size(); }

  int read(uint64_t offset, uint64_t len, ceph::buffer::list &bl) override;
  int write(uint64_t offset, const ceph::buffer::list &bl) override;
  int clone_range(MemObject *o, uint64_t srcoff, uint64_t len,
                  uint64_t dstoff) override;
  int truncate(uint64_t offset) override;

  void encode(ceph::buffer::list& bl) const override {
    ENCODE_START(1, 1, bl);
    encode(data, bl);
    encode_base(bl);
    ENCODE_FINISH(bl);
  }
  void decode(buffer::list::const_iterator& p) override {
    DECODE_START(1, p);
    decode(data, p);
    decode_base(p);
    DECODE_FINISH(p);
  }

private:
  int _write(uint64_t offset, std::vector<char>& buf);
};


struct PageSetObject : public MemObject {
  PageSet data;
  uint64_t data_len;
#if defined(__GLIBCXX__)
  // use a thread-local vector for the pages returned by PageSet, so we
  // can avoid allocations in read/write()
  static thread_local PageSet::page_vector tls_pages;
#endif

  size_t get_size() const override { return data_len; }

  int read(uint64_t offset, uint64_t len, ceph::buffer::list &bl) override;
  int write(uint64_t offset, const ceph::buffer::list &bl) override;
  int clone_range(MemObject *src, uint64_t srcoff, uint64_t len,
                  uint64_t dstoff) override;
  int truncate(uint64_t offset) override;

  void encode(ceph::buffer::list& bl) const override {
    ENCODE_START(1, 1, bl);
    encode(data_len, bl);
    data.encode(bl);
    encode_base(bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& p) override {
    DECODE_START(1, p);
    decode(data_len, p);
    data.decode(p);
    decode_base(p);
    DECODE_FINISH(p);
  }

private:
  FRIEND_MAKE_REF(PageSetObject);
  explicit PageSetObject(size_t page_size) : data(page_size), data_len(0) {}
};

#endif
