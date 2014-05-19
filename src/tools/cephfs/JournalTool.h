// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 John Spray <john.spray@inktank.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "MDSUtility.h"
#include <vector>

#include "mds/mdstypes.h"
#include "mds/LogEvent.h"

#include "include/rados/librados.hpp"

#include "JournalFilter.h"

class EMetaBlob;
class JournalScanner;


/**
 * Command line tool for investigating and repairing filesystems
 * with damaged metadata logs
 */
class JournalTool : public MDSUtility
{
  private:
    int rank;

    // Entry points
    int main_journal(std::vector<const char*> &argv);
    int main_header(std::vector<const char*> &argv);
    int main_event(std::vector<const char*> &argv);

    // Shared functionality
    int recover_journal();

    // Journal operations
    int journal_inspect();
    int journal_export(std::string const &path, bool import);
    int journal_reset();

    // Header operations
    int header_set();

    // I/O handles
    librados::Rados rados;
    librados::IoCtx io;

    // Metadata backing store manipulation
    int replay_offline(EMetaBlob const &metablob, bool const dry_run);

    // Splicing
    int erase_region(JournalScanner const &jp, uint64_t const pos, uint64_t const length);

  public:
    void usage();
    JournalTool() :
      rank(0) {}
    int main(std::vector<const char*> &argv);
};

