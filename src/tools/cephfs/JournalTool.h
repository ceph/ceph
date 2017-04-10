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
#include "RoleSelector.h"
#include <vector>

#include "mds/mdstypes.h"
#include "mds/LogEvent.h"
#include "mds/events/EMetaBlob.h"

#include "include/rados/librados.hpp"

#include "JournalFilter.h"

class JournalScanner;


/**
 * Command line tool for investigating and repairing filesystems
 * with damaged metadata logs
 */
class JournalTool : public MDSUtility
{
  private:
    MDSRoleSelector role_selector;
    // Bit hacky, use this `rank` member to control behaviour of the
    // various main_ functions.
    mds_rank_t rank;

    // Entry points
    int main_journal(std::vector<const char*> &argv);
    int main_header(std::vector<const char*> &argv);
    int main_event(std::vector<const char*> &argv);

    // Shared functionality
    int recover_journal();

    // Journal operations
    int journal_inspect();
    int journal_export(std::string const &path, bool import);
    int journal_reset(bool hard);

    // Header operations
    int header_set();

    // I/O handles
    librados::Rados rados;
    librados::IoCtx input;
    librados::IoCtx output;

    bool other_pool;

    // Metadata backing store manipulation
    int read_lost_found(std::set<std::string> &lost);
    int scavenge_dentries(
        EMetaBlob const &metablob,
        bool const dry_run,
        std::set<inodeno_t> *consumed_inos);
    int replay_offline(EMetaBlob const &metablob, bool const dry_run);

    // Splicing
    int erase_region(JournalScanner const &jp, uint64_t const pos, uint64_t const length);

    // Backing store helpers
    void encode_fullbit_as_inode(
        const EMetaBlob::fullbit &fb,
        const bool bare,
        bufferlist *out_bl);
    int consume_inos(const std::set<inodeno_t> &inos);

  public:
    void usage();
    JournalTool() :
      rank(0), other_pool(false) {}
    int main(std::vector<const char*> &argv);
};

