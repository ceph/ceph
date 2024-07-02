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

#include "mds/events/ESubtreeMap.h"
#include "mds/events/ESession.h"
#include "mds/events/ESessions.h"

#include "mds/events/EMetaBlob.h"
#include "mds/events/EResetJournal.h"
#include "mds/events/ENoOp.h"

#include "mds/events/EUpdate.h"
#include "mds/events/EPeerUpdate.h"
#include "mds/events/EOpen.h"
#include "mds/events/ECommitted.h"
#include "mds/events/EPurged.h"

#include "mds/events/EExport.h"
#include "mds/events/EImportStart.h"
#include "mds/events/EImportFinish.h"
#include "mds/events/EFragment.h"

#include "mds/events/ELid.h"
#include "mds/events/ESegment.h"
#include "mds/events/ETableClient.h"
#include "mds/events/ETableServer.h"

#include "journal.h"


JournalLogSegmentExpirationHook journal_log_segment_expiration_hook;

void LogSegment::try_to_expire(MDSRankBase *mdsb, MDSGatherBuilder &gather_bld, int op_prio)
{
  if (auto hook = journal_log_segment_expiration_hook) {
    hook(*this, mdsb, gather_bld, op_prio);
  }
}

void EMetaBlob::update_segment(LogSegment *ls)
{

}

/**
 * Get all inodes touched by this metablob.  Includes the 'bits' within
 * dirlumps, and the inodes of the dirs themselves.
 */
void EMetaBlob::get_inodes(
    std::set<inodeno_t> &inodes) const
{

}


/**
 * Get a map of dirfrag to set of dentries in that dirfrag which are
 * touched in this operation.
 */
void EMetaBlob::get_dentries(std::map<dirfrag_t, std::set<std::string> > &dentries) const
{

}


/**
 * Calculate all paths that we can infer are touched by this metablob.  Only uses
 * information local to this metablob so it may only be the path within the
 * subtree.
 */
void EMetaBlob::get_paths(
    std::vector<std::string> &paths) const
{

}

void EMetaBlob::replay(MDSRankBase *mdsb, LogSegment *logseg, int type, MDPeerUpdate *peerup)
{
  
}

// -----------------------
// EPurged
void EPurged::update_segment()
{

}

void EPurged::replay(MDSRankBase *mdsb)
{

}

// -----------------------
// ESession

void ESession::update_segment()
{

}

void ESession::replay(MDSRankBase *mdsb)
{

}

void ESessions::update_segment()
{

}

void ESessions::replay(MDSRankBase *mdsb)
{

}


// -----------------------
// ETableServer

void ETableServer::update_segment()
{

}

void ETableServer::replay(MDSRankBase *mdsb)
{

}


// ---------------------
// ETableClient

void ETableClient::replay(MDSRankBase *mds)
{

}

// -----------------------
// EUpdate

void EUpdate::update_segment()
{

}

void EUpdate::replay(MDSRankBase *mdsb)
{

}


// ------------------------
// EOpen

void EOpen::update_segment()
{
  // ??
}

void EOpen::replay(MDSRankBase *mds)
{

}


// -----------------------
// ECommitted

void ECommitted::replay(MDSRankBase *mds)
{

}


// -----------------------
// EPeerUpdate

void EPeerUpdate::replay(MDSRankBase *mds)
{

}


// -----------------------
// ESubtreeMap

void ESubtreeMap::replay(MDSRankBase *mdsb) 
{

}



// -----------------------
// EFragment

void EFragment::replay(MDSRankBase *mdsb)
{

}


// =========================================================================

// -----------------------
// EExport

void EExport::replay(MDSRankBase *mds)
{

}

// -----------------------
// EImportStart

void EImportStart::update_segment()
{

}

void EImportStart::replay(MDSRankBase *mdsb)
{

}

// -----------------------
// EImportFinish

void EImportFinish::replay(MDSRankBase *mds)
{

}

// ------------------------
// EResetJournal

void EResetJournal::replay(MDSRankBase *mdsb)
{

}

void ESegment::replay(MDSRankBase *mds)
{

}

void ELid::replay(MDSRankBase *mds)
{

}

void ENoOp::replay(MDSRankBase *mds)
{

}

/**
 * If re-formatting an old journal that used absolute log position
 * references as segment sequence numbers, use this function to update
 * it.
 *
 * @param mds
 * MDSRank instance, just used for logging
 * @param old_to_new
 * Map of old journal segment sequence numbers to new journal segment sequence numbers
 *
 * @return
 * True if the event was modified.
 */
bool EMetaBlob::rewrite_truncate_finish(MDSRankBase const *mds,
    std::map<LogSegment::seq_t, LogSegment::seq_t> const &old_to_new)
{
  return false;
}
