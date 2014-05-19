// -*- mode:c++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * ceph - scalable distributed file system
 *
 * copyright (c) 2014 john spray <john.spray@inktank.com>
 *
 * this is free software; you can redistribute it and/or
 * modify it under the terms of the gnu lesser general public
 * license version 2.1, as published by the free software
 * foundation.  see file copying.
 */


#include "JournalFilter.h"

#include "common/ceph_argparse.h"

#include "mds/events/ESession.h"
#include "mds/events/EUpdate.h"

#define dout_subsys ceph_subsys_mds


const string JournalFilter::range_separator("..");


/*
 * Return whether a LogEvent is to be included or excluded.
 *
 * The filter parameters are applied on an AND basis: if any
 * condition is not met, the event is excluded.  Try to do
 * the fastest checks first.
 */
bool JournalFilter::apply(uint64_t pos, LogEvent &le) const
{
  /* Filtering by journal offset range */
  if (pos < range_start || pos >= range_end) {
    return false;
  }

  /* Filtering by event type */
  if (event_type != 0) {
    if (le.get_type() != event_type) {
      return false;
    }
  }

  /* Filtering by client */
  if (client_name.num()) {
    EMetaBlob const *metablob = le.get_metablob();
    if (metablob) {
      if (metablob->get_client_name() != client_name) {
        return false;
      }
    } else if (le.get_type() == EVENT_SESSION) {
      ESession *es = reinterpret_cast<ESession*>(&le);
      if (es->get_client_inst().name != client_name) {
        return false;
      }
    } else {
      return false;
    }
  }

  /* Filtering by inode */
  if (inode) {
    EMetaBlob const *metablob = le.get_metablob();
    if (metablob) {
      std::set<inodeno_t> inodes;
      metablob->get_inodes(inodes);
      bool match_any = false;
      for (std::set<inodeno_t>::iterator i = inodes.begin(); i != inodes.end(); ++i) {
        if (*i == inode) {
          match_any = true;
          break;
        }
      }
      if (!match_any) {
        return false;
      }
    } else {
      return false;
    }
  }

  /* Filtering by frag and dentry */
  if (!frag_dentry.empty() || frag.ino) {
    EMetaBlob const *metablob = le.get_metablob();
    if (metablob) {
      std::map<dirfrag_t, std::set<std::string> > dentries;
      metablob->get_dentries(dentries);

      if (frag.ino) {
        bool match_any = false;
        for (std::map<dirfrag_t, std::set<std::string> >::iterator i = dentries.begin();
            i != dentries.end(); ++i) {
          if (i->first == frag) {
            match_any = true;
            break;
          }
        }
        if (!match_any) {
          return false;
        }
      }

      if (!frag_dentry.empty()) {
        bool match_any = false;
        for (std::map<dirfrag_t, std::set<std::string> >::iterator i = dentries.begin();
            i != dentries.end() && !match_any; ++i) {
          std::set<std::string> const &names = i->second;
          for (std::set<std::string>::iterator j = names.begin();
              j != names.end() && !match_any; ++j) {
            if (*j == frag_dentry) {
              match_any = true;
            }
          }
        }
        if (!match_any) {
          return false;
        }
      }

    } else {
      return false;
    }
  }

  /* Filtering by file path */
  if (!path_expr.empty()) {
    EMetaBlob const *metablob = le.get_metablob();
    if (metablob) {
      std::vector<std::string> paths;
      metablob->get_paths(paths);
      bool match_any = false;
      for (std::vector<std::string>::iterator p = paths.begin(); p != paths.end(); ++p) {
        if ((*p).find(path_expr) != std::string::npos) {
          match_any = true;
          break;
        }
      }
      if (!match_any) {
        return false;
      }
    } else {
      return false;
    }
  }

  return true;
}


int JournalFilter::parse_args(
  std::vector<const char*> &argv, 
  std::vector<const char*>::iterator &arg)
{
  while(arg != argv.end()) {
    std::string arg_str;
    if (ceph_argparse_witharg(argv, arg, &arg_str, "--range", (char*)NULL)) {
      size_t sep_loc = arg_str.find(JournalFilter::range_separator);
      if (sep_loc == std::string::npos || arg_str.size() <= JournalFilter::range_separator.size()) {
        derr << "Invalid range '" << arg_str << "'" << dendl;
        return -EINVAL;
      }

      // We have a lower bound
      if (sep_loc > 0) {
        std::string range_start_str = arg_str.substr(0, sep_loc); 
        std::string parse_err;
        range_start = strict_strtoll(range_start_str.c_str(), 0, &parse_err);
        if (!parse_err.empty()) {
          derr << "Invalid lower bound '" << range_start_str << "': " << parse_err << dendl;
          return -EINVAL;
        }
      }

      if (sep_loc < arg_str.size() - JournalFilter::range_separator.size()) {
        std::string range_end_str = arg_str.substr(sep_loc + range_separator.size()); 
        std::string parse_err;
        range_end = strict_strtoll(range_end_str.c_str(), 0, &parse_err);
        if (!parse_err.empty()) {
          derr << "Invalid upper bound '" << range_end_str << "': " << parse_err << dendl;
          return -EINVAL;
        }
      }
    } else if (ceph_argparse_witharg(argv, arg, &arg_str, "--path", (char*)NULL)) {
      dout(4) << "Filtering by path '" << arg_str << "'" << dendl;
      path_expr = arg_str;
    } else if (ceph_argparse_witharg(argv, arg, &arg_str, "--inode", (char*)NULL)) {
      dout(4) << "Filtering by inode '" << arg_str << "'" << dendl;
      std::string parse_err;
      inode = strict_strtoll(arg_str.c_str(), 0, &parse_err);
      if (!parse_err.empty()) {
        derr << "Invalid inode '" << arg_str << "': " << parse_err << dendl;
        return -EINVAL;
      }
    } else if (ceph_argparse_witharg(argv, arg, &arg_str, "--type", (char*)NULL)) {
      std::string parse_err;
      event_type = LogEvent::str_to_type(arg_str);
      if (event_type == LogEvent::EventType(-1)) {
        derr << "Invalid event type '" << arg_str << "': " << parse_err << dendl;
        return -EINVAL;
      }

    } else if (ceph_argparse_witharg(argv, arg, &arg_str, "--frag", (char*)NULL)) {
      std::string const frag_sep = ".";
      size_t sep_loc = arg_str.find(frag_sep);
      std::string inode_str;
      std::string frag_str;
      if (sep_loc != std::string::npos) {
        inode_str = arg_str.substr(0, sep_loc);
        frag_str = arg_str.substr(sep_loc + 1);
      } else {
        inode_str = arg_str;
        frag_str = "0";
      }

      std::string parse_err;
      inodeno_t frag_ino = strict_strtoll(inode_str.c_str(), 0, &parse_err);
      if (!parse_err.empty()) {
        derr << "Invalid inode '" << inode_str << "': " << parse_err << dendl;
        return -EINVAL;
      }

      uint32_t frag_enc = strict_strtoll(frag_str.c_str(), 0, &parse_err);
      if (!parse_err.empty()) {
        derr << "Invalid frag '" << frag_str << "': " << parse_err << dendl;
        return -EINVAL;
      }

      frag = dirfrag_t(frag_ino, frag_t(frag_enc));
      dout(4) << "dirfrag filter: '" << frag << "'" << dendl;
    } else if (ceph_argparse_witharg(argv, arg, &arg_str, "--dname", (char*)NULL)) {
      frag_dentry = arg_str;
      dout(4) << "dentry filter: '" << frag_dentry << "'" << dendl;
    } else if (ceph_argparse_witharg(argv, arg, &arg_str, "--client", (char*)NULL)) {
      std::string parse_err;
      int64_t client_num = strict_strtoll(arg_str.c_str(), 0, &parse_err);
      if (!parse_err.empty()) {
        derr << "Invalid client number " << arg_str << dendl;
        return -EINVAL;
      }
      client_name = entity_name_t::CLIENT(client_num);
    } else {
      // We're done with args the filter understands
      break;
    }
  }

  return 0;
}

/**
 * If the filter params are only range, then return
 * true and set start & end.  Else return false.
 *
 * Use this to discover if the user has requested a contiguous range
 * rather than any per-event filtering.
 */
bool JournalFilter::get_range(uint64_t &start, uint64_t &end) const
{
  if (!path_expr.empty()
      || inode != 0
      || event_type != 0
      || frag.ino != 0
      || client_name.num() != 0
      || (range_start == 0 && range_end == (uint64_t)(-1))) {
    return false;
  } else {
    start = range_start;
    end = range_end;
    return true;
  }
}
