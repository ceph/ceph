// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_GROUP_LIST_SNAPSHOTS_REQUEST_H
#define CEPH_LIBRBD_GROUP_LIST_SNAPSHOTS_REQUEST_H

#include "include/int_types.h"
#include "include/types.h"
#include "include/rados/librados.hpp"
#include "cls/rbd/cls_rbd_types.h"

#include <string>
#include <vector>

class Context;

namespace librbd {

struct ImageCtx;

namespace group {

template <typename ImageCtxT = librbd::ImageCtx>
class ListSnapshotsRequest {
public:
  static ListSnapshotsRequest *create(
      librados::IoCtx &group_io_ctx, const std::string &group_id,
      bool try_to_sort, bool fail_if_not_sorted,
      std::vector<cls::rbd::GroupSnapshot> *snaps, Context *on_finish) {
    return new ListSnapshotsRequest(group_io_ctx, group_id, try_to_sort,
                                    fail_if_not_sorted, snaps, on_finish);
  }

  ListSnapshotsRequest(librados::IoCtx &group_io_ctx,
                       const std::string &group_id,
                       bool try_to_sort, bool fail_if_not_sorted,
                       std::vector<cls::rbd::GroupSnapshot> *snaps,
                       Context *on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   * <start>    /--------\
   *    |       |        | (if requested, repeat if more
   *    v       v        |  entries)
   *  LIST_SNAP_ORDERS --/
   *    |
   *    |       /--------\
   *    |       |        | (repeat if more
   *    v       v        |  snapshots)
   *  LIST_SNAPS --------/
   *    |
   *    |                          /--------\
   *    |/-------<--------\        |        | (repeat if more
   *    |                 |        v        |  entries)
   *    |                LIST_SNAP_ORDERS --/
   *    |                 ^
   *    |                 | (retry if ordering is required and
   *    |                 |  an entry is missing for a snapshot)
   *    v (if requested)  |
   *  SORT_SNAPS ---------/
   *    |
   *    v
   *  <finish>
   *
   * @endverbatim
   */

  librados::IoCtx &m_group_io_ctx;
  std::string m_group_id;
  bool m_try_to_sort;
  //Fail if m_try_to_sort is true and sorting fails. Ignored if m_try_to_sort is false.
  bool m_fail_if_not_sorted;
  std::vector<cls::rbd::GroupSnapshot> *m_snaps;
  std::map<std::string, uint64_t> m_snap_orders;
  Context *m_on_finish;

  cls::rbd::GroupSnapshot m_start_after;
  std::string m_start_after_order;
  bufferlist m_out_bl;
  bool m_retried_snap_orders = false;

  void list_snaps();
  void handle_list_snaps(int r);

  void list_snap_orders();
  void handle_list_snap_orders(int r);

  void sort_snaps();

  void finish(int r);
};

} // namespace group
} // namespace librbd

extern template class librbd::group::ListSnapshotsRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_GROUP_LIST_SNAPSHOTS_REQUEST_H
