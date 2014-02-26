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


#include "mds/MDSUtility.h"
#include "osdc/Journaler.h"

/**
 * This class lets you dump out an mds journal for troubleshooting or whatever.
 *
 * It was built to work with cmds so some of the design choices are random.
 * To use, create a Dumper, call init(), and then call dump() with the name
 * of the file to dump to.
 */

class Dumper : public MDSUtility {
private:
  Journaler *journaler;
  int rank;

public:
  Dumper() : journaler(NULL), rank(-1)
  {}

  void handle_mds_map(MMDSMap* m);

  int init(int rank);
  int recover_journal();
  void dump(const char *dumpfile);
  void undump(const char *dumpfile);
  void dump_entries();
};

#endif /* JOURNAL_DUMPER_H_ */
