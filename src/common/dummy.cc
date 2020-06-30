// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Inktank, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

/*
 * A dummy file with a .cc extension to make autotools link
 * ceph_test_librbd_fsx with a C++ linker.  An approach w/o a physical
 * dummy.cc recommended in 8.3.5 Libtool Convenience Libraries works,
 * but breaks 'make tags' and friends.
 */
