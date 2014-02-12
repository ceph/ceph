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


#include "osdc/Journaler.h"
#include "mds/MDSUtility.h"

/**
 * This class lets you reset an mds journal for troubleshooting or whatever.
 *
 * To use, create a Resetter, call init(), and then call reset() with the name
 * of the file to dump to.
 */
class Resetter : public MDSUtility {
public:
  Journaler *journaler;

  Resetter() : journaler(NULL) {}

  int init(int rank);
  void reset();
};

#endif /* JOURNAL_RESETTER_H_ */
