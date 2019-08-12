// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


/*
 * This test is linked against librados and libcephfs to try and detect issues
 * with global, static, non-POD variables as seen in the following trackers.
 * http://tracker.ceph.com/issues/16504
 * http://tracker.ceph.com/issues/16686
 * In those trackers such variables caused segfaults with glibc reporting
 * "double free or corruption".
 *
 * Don't be fooled by its emptiness. It does serve a purpose :)
 */

int main(int, char**)
{
    return 0;
}
