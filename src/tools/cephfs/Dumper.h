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

#ifndef JOURNAL_DUMPER_H_
#define JOURNAL_DUMPER_H_


#include "MDSUtility.h"

class Journaler;

/**
 * This class lets you dump out an mds journal for troubleshooting or whatever.
 *
 * It was built to work with cmds so some of the design choices are random.
 * To use, create a Dumper, call init(), and then call dump() with the name
 * of the file to dump to.
 */

class Dumper : public MDSUtility {
private:
  mds_role_t role;
  inodeno_t ino;

public:
  Dumper() : ino(-1)
  {}

  int init(mds_role_t role_);
  int recover_journal(Journaler *journaler);
  int dump(const char *dumpfile);
  int undump(const char *dumpfile);
};

#endif /* JOURNAL_DUMPER_H_ */
