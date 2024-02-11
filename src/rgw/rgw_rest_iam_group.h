// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include "include/buffer_fwd.h"

class RGWOp;

// IAM Group op factory functions
RGWOp* make_iam_create_group_op(const ceph::bufferlist& post_body);
RGWOp* make_iam_get_group_op(const ceph::bufferlist& unused);
RGWOp* make_iam_update_group_op(const ceph::bufferlist& post_body);
RGWOp* make_iam_delete_group_op(const ceph::bufferlist& post_body);
RGWOp* make_iam_list_groups_op(const ceph::bufferlist& unused);

RGWOp* make_iam_add_user_to_group_op(const ceph::bufferlist& post_body);
RGWOp* make_iam_remove_user_from_group_op(const ceph::bufferlist& post_body);
RGWOp* make_iam_list_groups_for_user_op(const ceph::bufferlist& unused);

// IAM GroupPolicy op factory functions
RGWOp* make_iam_put_group_policy_op(const ceph::bufferlist& post_body);
RGWOp* make_iam_get_group_policy_op(const ceph::bufferlist& unused);
RGWOp* make_iam_delete_group_policy_op(const ceph::bufferlist& post_body);
RGWOp* make_iam_list_group_policies_op(const ceph::bufferlist& unused);
RGWOp* make_iam_attach_group_policy_op(const ceph::bufferlist& post_body);
RGWOp* make_iam_detach_group_policy_op(const ceph::bufferlist& post_body);
RGWOp* make_iam_list_attached_group_policies_op(const ceph::bufferlist& unused);
