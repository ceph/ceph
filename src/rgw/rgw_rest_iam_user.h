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

// IAM User op factory functions
RGWOp* make_iam_create_user_op(const ceph::bufferlist& post_body);
RGWOp* make_iam_get_user_op(const ceph::bufferlist& unused);
RGWOp* make_iam_update_user_op(const ceph::bufferlist& post_body);
RGWOp* make_iam_delete_user_op(const ceph::bufferlist& post_body);
RGWOp* make_iam_list_users_op(const ceph::bufferlist& unused);

// AccessKey op factory functions
RGWOp* make_iam_create_access_key_op(const ceph::bufferlist& post_body);
RGWOp* make_iam_update_access_key_op(const ceph::bufferlist& post_body);
RGWOp* make_iam_delete_access_key_op(const ceph::bufferlist& post_body);
RGWOp* make_iam_list_access_keys_op(const ceph::bufferlist& unused);
