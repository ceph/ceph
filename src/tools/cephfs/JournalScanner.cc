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


#include "include/rados/librados.hpp"
#include "mds/JournalPointer.h"

#include "mds/events/ESubtreeMap.h"
#include "mds/PurgeQueue.h"

#include "JournalScanner.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds

/**
 * Read journal header, followed by sequential scan through journal space.
 *
 * Return 0 on success, else error code.  Note that success has the special meaning
 * that we were able to apply our checks, it does *not* mean that the journal is
 * healthy.
 */
int JournalScanner::scan(bool const full)
{
  int r = 0;

  r = set_journal_ino();
  if (r < 0) {
    return r;
  }

  if (!is_mdlog || pointer_present) {
    r = scan_header();
    if (r < 0) {
      return r;
    }
  }

  if (full && header_present) {
    r = scan_events();
    if (r < 0) {
      return r;
    }
  }

  return 0;
}


int JournalScanner::set_journal_ino()
{
  int r = 0;
  if (type == "purge_queue") {
    ino = MDS_INO_PURGE_QUEUE + rank;
  }
  else if (type == "mdlog"){
    r = scan_pointer();
    is_mdlog = true;
  }
  else {
    ceph_abort(); // should not get here
  }
  return r;
}

int JournalScanner::scan_pointer()
{
  // Issue read
  std::string const pointer_oid = obj_name(MDS_INO_LOG_POINTER_OFFSET + rank, 0);
  bufferlist pointer_bl;
  int r = io.read(pointer_oid, pointer_bl, INT_MAX, 0);
  if (r == -ENOENT) {
    // 'Successfully' discovered the pointer is missing.
    derr << "Pointer " << pointer_oid << " is absent" << dendl;
    return 0;
  } else if (r < 0) {
    // Error preventing us interrogating pointer
    derr << "Pointer " << pointer_oid << " is unreadable" << dendl;
    return r;
  } else {
    dout(4) << "Pointer " << pointer_oid << " is readable" << dendl;
    pointer_present = true;

    JournalPointer jp;
    try {
      auto q = pointer_bl.cbegin();
      jp.decode(q);
    } catch(buffer::error &e) {
      derr << "Pointer " << pointer_oid << " is corrupt: " << e.what() << dendl;
      return 0;
    }

    pointer_valid = true;
    ino = jp.front;
    return 0;
  }
}


int JournalScanner::scan_header()
{
  int r;

  bufferlist header_bl;
  std::string header_name = obj_name(0);
  dout(4) << "JournalScanner::scan: reading header object '" << header_name << "'" << dendl;
  r = io.read(header_name, header_bl, INT_MAX, 0);
  if (r < 0) {
    derr << "Header " << header_name << " is unreadable" << dendl;
    return 0;  // "Successfully" found an error
  } else {
    header_present = true;
  }

  auto header_bl_i = header_bl.cbegin();
  header = new Journaler::Header();
  try
  {
    header->decode(header_bl_i);
  }
  catch (buffer::error &e)
  {
    derr << "Header is corrupt (" << e.what() << ")" << dendl;
    delete header;
    header = NULL;
    return 0;  // "Successfully" found an error
  }

  if (header->magic != std::string(CEPH_FS_ONDISK_MAGIC)) {
    derr << "Header is corrupt (bad magic)" << dendl;
    return 0;  // "Successfully" found an error
  }
  if (!((header->trimmed_pos <= header->expire_pos) && (header->expire_pos <= header->write_pos))) {
    derr << "Header is invalid (inconsistent offsets)" << dendl;
    return 0;  // "Successfully" found an error
  }
  header_valid = true;

  return 0;
}


int JournalScanner::scan_events()
{
  uint64_t object_size = g_conf()->mds_log_segment_size;
  if (object_size == 0) {
    // Default layout object size
    object_size = file_layout_t::get_default().object_size;
  }

  uint64_t read_offset = header->expire_pos;
  dout(10) << std::hex << "Header 0x"
    << header->trimmed_pos << " 0x"
    << header->expire_pos << " 0x"
    << header->write_pos << std::dec << dendl;
  dout(10) << "Starting journal scan from offset 0x" << std::hex << read_offset << std::dec << dendl;

  // TODO also check for extraneous objects before the trimmed pos or after the write pos,
  // which would indicate a bogus header.

  bufferlist read_buf;
  bool gap = false;
  uint64_t gap_start = -1;
  for (uint64_t obj_offset = (read_offset / object_size); ; obj_offset++) {
    uint64_t offset_in_obj = 0;
    if (obj_offset * object_size < header->expire_pos) {
      // Skip up to expire_pos from start of the object
      // (happens for the first object we read)
      offset_in_obj = header->expire_pos - obj_offset * object_size;
    }

    // Read this journal segment
    bufferlist this_object;
    std::string const oid = obj_name(obj_offset);
    int r = io.read(oid, this_object, INT_MAX, offset_in_obj);

    // Handle absent journal segments
    if (r < 0) {
      if (obj_offset > (header->write_pos / object_size)) {
        dout(4) << "Reached end of journal objects" << dendl;
        break;
      } else {
        derr << "Missing object " << oid << dendl;
      }

      objects_missing.push_back(obj_offset);
      gap = true;
      gap_start = read_offset;
      continue;
    } else {
      dout(4) << "Read 0x" << std::hex << this_object.length() << std::dec
              << " bytes from " << oid << " gap=" << gap << dendl;
      objects_valid.push_back(oid);
      this_object.copy(0, this_object.length(), read_buf);
    }

    if (gap) {
      // No valid data at the current read offset, scan forward until we find something valid looking
      // or have to drop out to load another object.
      dout(4) << "Searching for sentinel from 0x" << std::hex << read_offset
              << ", 0x" << read_buf.length() << std::dec << " bytes available" << dendl;

      do {
        auto p = read_buf.cbegin();
        uint64_t candidate_sentinel;
        decode(candidate_sentinel, p);

        dout(4) << "Data at 0x" << std::hex << read_offset << " = 0x" << candidate_sentinel << std::dec << dendl;

        if (candidate_sentinel == JournalStream::sentinel) {
          dout(4) << "Found sentinel at 0x" << std::hex << read_offset << std::dec << dendl;
          ranges_invalid.push_back(Range(gap_start, read_offset));
          gap = false;
          break;
        } else {
          // No sentinel, discard this byte
          read_buf.splice(0, 1);
          read_offset += 1;
        }
      } while (read_buf.length() >= sizeof(JournalStream::sentinel));
      dout(4) << "read_buf size is " << read_buf.length() << dendl;
    } else {
      dout(10) << "Parsing data, 0x" << std::hex << read_buf.length() << std::dec << " bytes available" << dendl;
      while(true) {
        // TODO: detect and handle legacy format journals: can do many things
        // on them but on read errors have to give up instead of searching
        // for sentinels.
        JournalStream journal_stream(JOURNAL_FORMAT_RESILIENT);
        bool readable = false;
        try {
          uint64_t need;
          readable = journal_stream.readable(read_buf, &need);
        } catch (buffer::error &e) {
          readable = false;
          dout(4) << "Invalid container encoding at 0x" << std::hex << read_offset << std::dec << dendl;
          gap = true;
          gap_start = read_offset;
          read_buf.splice(0, 1);
          read_offset += 1;
          break;
        }

        if (!readable) {
          // Out of data, continue to read next object
          break;
        }

        bufferlist le_bl;  //< Serialized LogEvent blob
        dout(10) << "Attempting decode at 0x" << std::hex << read_offset << std::dec << dendl;
        // This cannot fail to decode because we pre-checked that a serialized entry
        // blob would be readable.
        uint64_t start_ptr = 0;
        uint64_t consumed = journal_stream.read(read_buf, &le_bl, &start_ptr);
        dout(10) << "Consumed 0x" << std::hex << consumed << std::dec << " bytes" << dendl;
        if (start_ptr != read_offset) {
          derr << "Bad entry start ptr (0x" << std::hex << start_ptr << ") at 0x"
              << read_offset << std::dec << dendl;
          gap = true;
          gap_start = read_offset;
          // FIXME: given that entry was invalid, should we be skipping over it?
          // maybe push bytes back onto start of read_buf and just advance one byte
          // to start scanning instead.  e.g. if a bogus size value is found it can
          // cause us to consume and thus skip a bunch of following valid events.
          read_offset += consumed;
          break;
        }
        bool valid_entry = true;
        if (is_mdlog) {
          auto le = LogEvent::decode_event(le_bl.cbegin());

          if (le) {
            dout(10) << "Valid entry at 0x" << std::hex << read_offset << std::dec << dendl;

            if (le->get_type() == EVENT_SUBTREEMAP
                || le->get_type() == EVENT_SUBTREEMAP_TEST) {
              auto&& sle = dynamic_cast<ESubtreeMap&>(*le);
              if (sle.expire_pos > read_offset) {
                errors.insert(std::make_pair(
                      read_offset, EventError(
                        -ERANGE,
                        "ESubtreeMap has expire_pos ahead of its own position")));
              }
            }

            if (filter.apply(read_offset, *le)) {
              events.insert_or_assign(read_offset, EventRecord(std::move(le), consumed));
            }
          } else {
            valid_entry = false;
          }
        } else if (type == "purge_queue"){
           auto pi = std::make_unique<PurgeItem>();
           try {
             auto q = le_bl.cbegin();
             pi->decode(q);
	     if (filter.apply(read_offset, *pi)) {
	       events.insert_or_assign(read_offset, EventRecord(std::move(pi), consumed));
	     }
           } catch (const buffer::error &err) {
             valid_entry = false;
           }
        } else {
          ceph_abort(); // should not get here
        }
        if (!valid_entry) {
          dout(10) << "Invalid entry at 0x" << std::hex << read_offset << std::dec << dendl;
          gap = true;
          gap_start = read_offset;
          read_offset += consumed;
          break;
        } else {
          events_valid.push_back(read_offset);
          read_offset += consumed;
        }
      }
    }
  }

  if (gap) {
    // Ended on a gap, assume it ran to end
    ranges_invalid.push_back(Range(gap_start, -1));
  }

  dout(4) << "Scanned objects, " << objects_missing.size() << " missing, " << objects_valid.size() << " valid" << dendl;
  dout(4) << "Events scanned, " << ranges_invalid.size() << " gaps" << dendl;
  dout(4) << "Found " << events_valid.size() << " valid events" << dendl;
  dout(4) << "Selected " << events.size() << " events events for processing" << dendl;

  return 0;
}


JournalScanner::~JournalScanner()
{
  if (header) {
    delete header;
    header = NULL;
  }
  dout(4) << events.size() << " events" << dendl;
  events.clear();
}


/**
 * Whether the journal data looks valid and replayable
 */
bool JournalScanner::is_healthy() const
{
  return ((!is_mdlog || (pointer_present && pointer_valid))
      && header_present && header_valid
      && ranges_invalid.empty()
      && objects_missing.empty());
}


/**
 * Whether the journal data can be read from RADOS
 */
bool JournalScanner::is_readable() const
{
  return (header_present && header_valid && objects_missing.empty());
}


/**
 * Calculate the object name for a given offset
 */
std::string JournalScanner::obj_name(inodeno_t ino, uint64_t offset) const
{
  char name[60];
  snprintf(name, sizeof(name), "%llx.%08llx",
      (unsigned long long)(ino),
      (unsigned long long)offset);
  return std::string(name);
}


std::string JournalScanner::obj_name(uint64_t offset) const
{
  return obj_name(ino, offset);
}


/*
 * Write a human readable summary of the journal health
 */
void JournalScanner::report(std::ostream &out) const
{
  out << "Overall journal integrity: " << (is_healthy() ? "OK" : "DAMAGED") << std::endl;

  if (is_mdlog) {
    if (!pointer_present) {
      out << "Pointer not found" << std::endl;
    } else if (!pointer_valid) {
      out << "Pointer could not be decoded" << std::endl;
    }
  }
  if (!header_present) {
    out << "Header not found" << std::endl;
  } else if (!header_valid) {
    out << "Header could not be decoded" << std::endl;
  }

  if (objects_missing.size()) {
    out << "Objects missing:" << std::endl;
    for (std::vector<uint64_t>::const_iterator om = objects_missing.begin();
         om != objects_missing.end(); ++om) {
      out << "  0x" << std::hex << *om << std::dec << std::endl;
    }
  }

  if (ranges_invalid.size()) {
    out << "Corrupt regions:" << std::endl;
    for (std::vector<Range>::const_iterator r = ranges_invalid.begin();
         r != ranges_invalid.end(); ++r) {
      out << "  0x" << std::hex << r->first << "-" << r->second << std::dec << std::endl;
    }
  }
}

