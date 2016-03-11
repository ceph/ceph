// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2010 Greg Farnum <gregf@hq.newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef JOURNAL_RESETTER_H_
#define JOURNAL_RESETTER_H_


#include "MDSUtility.h"

class Journaler;

/**
 * This class lets you reset an mds journal for troubleshooting or whatever.
 *
 * To use, create a Resetter, call init(), and then call reset() with the name
 * of the file to dump to.
 */
class Resetter : public MDSUtility {
protected:
  int _write_reset_event(Journaler *journaler);

public:
  Resetter() {}

  /**
   * For use when no journal header/pointer was present: write one
   * out from scratch.
   */
  int reset_hard(mds_role_t role);
  int reset(mds_role_t role);
};

#endif /* JOURNAL_RESETTER_H_ */
