// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_JOURNAL_UTILS_H
#define CEPH_LIBRBD_JOURNAL_UTILS_H

#include "include/common_fwd.h"
#include "include/int_types.h"
#include "include/Context.h"
#include "cls/journal/cls_journal_types.h"
#include <list>


namespace librbd {
namespace journal {

struct TagData;

namespace util {

struct C_DecodeTag : public Context {
  CephContext *cct;
  ceph::mutex *lock;
  uint64_t *tag_tid;
  TagData *tag_data;
  Context *on_finish;

  cls::journal::Tag tag;

  C_DecodeTag(CephContext *cct, ceph::mutex *lock, uint64_t *tag_tid,
              TagData *tag_data, Context *on_finish)
    : cct(cct), lock(lock), tag_tid(tag_tid), tag_data(tag_data),
      on_finish(on_finish) {
  }

  void complete(int r) override {
    on_finish->complete(process(r));
    Context::complete(0);
  }
  void finish(int r) override {
  }

  int process(int r);

  static int decode(bufferlist::const_iterator *it, TagData *tag_data);

};

struct C_DecodeTags : public Context {
  typedef std::list<cls::journal::Tag> Tags;

  CephContext *cct;
  ceph::mutex *lock;
  uint64_t *tag_tid;
  TagData *tag_data;
  Context *on_finish;

  Tags tags;

  C_DecodeTags(CephContext *cct, ceph::mutex *lock, uint64_t *tag_tid,
               TagData *tag_data, Context *on_finish)
    : cct(cct), lock(lock), tag_tid(tag_tid), tag_data(tag_data),
      on_finish(on_finish) {
  }

  void complete(int r) override {
    on_finish->complete(process(r));
    Context::complete(0);
  }
  void finish(int r) override {
  }

  int process(int r);
};

} // namespace util
} // namespace journal
} // namespace librbd

#endif // CEPH_LIBRBD_JOURNAL_UTILS_H
