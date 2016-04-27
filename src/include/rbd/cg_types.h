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

#ifndef CEPH_CG_TYPES_H
#define CEPH_CG_TYPES_H

enum image_link_state {
	LINK_NORMAL,
	LINK_DIRTY
};

#define CG_SNAP_SEQ "snap_seq"
#define CG_STATE "state"
#define CG_IMAGE_TO_BE_ADDED "image_to_be_added"

#define CG_ID_PREFIX           "cg_id."
#define CG_HEADER_PREFIX       "cg_header."

#define CG_DIRECTORY           "cg_directory"

#endif
