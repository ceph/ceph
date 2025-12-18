// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_LIBRBD_JOURNAL_TYPE_TRAITS_H
#define CEPH_LIBRBD_JOURNAL_TYPE_TRAITS_H

struct ContextWQ;

namespace journal {
class Future;
class Journaler;
class ReplayEntry;
}

namespace librbd {
namespace journal {

template <typename ImageCtxT>
struct TypeTraits {
  typedef ::journal::Journaler Journaler;
  typedef ::journal::Future Future;
  typedef ::journal::ReplayEntry ReplayEntry;
  typedef ::ContextWQ ContextWQ;
};

} // namespace journal
} // namespace librbd

#endif // CEPH_LIBRBD_JOURNAL_TYPE_TRAITS_H
