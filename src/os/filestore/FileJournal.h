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


#ifndef CEPH_FILEJOURNAL_H
#define CEPH_FILEJOURNAL_H

#include <deque>
using std::deque;

#include "Journal.h"
#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/Thread.h"
#include "common/Throttle.h"
#include "JournalThrottle.h"


#ifdef HAVE_LIBAIO
# include <libaio.h>
#endif

/**
 * Implements journaling on top of block device or file.
 *
 * Lock ordering is write_lock > aio_lock > (completions_lock | finisher_lock)
 */
class FileJournal :
  public Journal,
  public md_config_obs_t {
public:
  /// Protected by finisher_lock
  struct completion_item {
    uint64_t seq;
    Context *finish;
    utime_t start;
    TrackedOpRef tracked_op;
    completion_item(uint64_t o, Context *c, utime_t s,
		    TrackedOpRef opref)
      : seq(o), finish(c), start(s), tracked_op(opref) {}
    completion_item() : seq(0), finish(0), start(0) {}
  };
  struct write_item {
    uint64_t seq;
    bufferlist bl;
    uint32_t orig_len;
    TrackedOpRef tracked_op;
    write_item(uint64_t s, bufferlist& b, int ol, TrackedOpRef opref) :
      seq(s), orig_len(ol), tracked_op(opref) {
      bl.claim(b, buffer::list::CLAIM_ALLOW_NONSHAREABLE); // potential zero-copy
    }
    write_item() : seq(0), orig_len(0) {}
  };

  Mutex finisher_lock;
  Cond finisher_cond;
  uint64_t journaled_seq;
  bool plug_journal_completions;

  Mutex writeq_lock;
  Cond writeq_cond;
  list<write_item> writeq;
  bool writeq_empty();
  write_item &peek_write();
  void pop_write();
  void batch_pop_write(list<write_item> &items);
  void batch_unpop_write(list<write_item> &items);

  Mutex completions_lock;
  list<completion_item> completions;
  bool completions_empty() {
    Mutex::Locker l(completions_lock);
    return completions.empty();
  }
  void batch_pop_completions(list<completion_item> &items) {
    Mutex::Locker l(completions_lock);
    completions.swap(items);
  }
  void batch_unpop_completions(list<completion_item> &items) {
    Mutex::Locker l(completions_lock);
    completions.splice(completions.begin(), items);
  }
  completion_item completion_peek_front() {
    Mutex::Locker l(completions_lock);
    assert(!completions.empty());
    return completions.front();
  }
  void completion_pop_front() {
    Mutex::Locker l(completions_lock);
    assert(!completions.empty());
    completions.pop_front();
  }

  int prepare_entry(vector<ObjectStore::Transaction>& tls, bufferlist* tbl);

  void submit_entry(uint64_t seq, bufferlist& bl, uint32_t orig_len,
		    Context *oncommit,
		    TrackedOpRef osd_op = TrackedOpRef());
  /// End protected by finisher_lock

  /*
   * journal header
   */
  struct header_t {
    enum {
      FLAG_CRC = (1<<0),
      // NOTE: remove kludgey weirdness in read_header() next time a flag is added.
    };

    uint64_t flags;
    uuid_d fsid;
    __u32 block_size;
    __u32 alignment;
    int64_t max_size;   // max size of journal ring buffer
    int64_t start;      // offset of first entry
    uint64_t committed_up_to; // committed up to

    /**
     * start_seq
     *
     * entry at header.start has sequence >= start_seq
     *
     * Generally, the entry at header.start will have sequence
     * start_seq if it exists.  The only exception is immediately
     * after journal creation since the first sequence number is
     * not known.
     *
     * If the first read on open fails, we can assume corruption
     * if start_seq > committed_up_thru because the entry would have
     * a sequence >= start_seq and therefore > committed_up_thru.
     */
    uint64_t start_seq;

    header_t() :
      flags(0), block_size(0), alignment(0), max_size(0), start(0),
      committed_up_to(0), start_seq(0) {}

    void clear() {
      start = block_size;
    }

    uint64_t get_fsid64() const {
      return *(uint64_t*)fsid.bytes();
    }

    void encode(bufferlist& bl) const {
      __u32 v = 4;
      ::encode(v, bl);
      bufferlist em;
      {
	::encode(flags, em);
	::encode(fsid, em);
	::encode(block_size, em);
	::encode(alignment, em);
	::encode(max_size, em);
	::encode(start, em);
	::encode(committed_up_to, em);
	::encode(start_seq, em);
      }
      ::encode(em, bl);
    }
    void decode(bufferlist::iterator& bl) {
      __u32 v;
      ::decode(v, bl);
      if (v < 2) {  // normally 0, but concievably 1
	// decode old header_t struct (pre v0.40).
	bl.advance(4); // skip __u32 flags (it was unused by any old code)
	flags = 0;
	uint64_t tfsid;
	::decode(tfsid, bl);
	*(uint64_t*)&fsid.bytes()[0] = tfsid;
	*(uint64_t*)&fsid.bytes()[8] = tfsid;
	::decode(block_size, bl);
	::decode(alignment, bl);
	::decode(max_size, bl);
	::decode(start, bl);
	committed_up_to = 0;
	start_seq = 0;
	return;
      }
      bufferlist em;
      ::decode(em, bl);
      bufferlist::iterator t = em.begin();
      ::decode(flags, t);
      ::decode(fsid, t);
      ::decode(block_size, t);
      ::decode(alignment, t);
      ::decode(max_size, t);
      ::decode(start, t);

      if (v > 2)
	::decode(committed_up_to, t);
      else
	committed_up_to = 0;

      if (v > 3)
	::decode(start_seq, t);
      else
	start_seq = 0;
    }
  } header;

  struct entry_header_t {
    uint64_t seq;     // fs op seq #
    uint32_t crc32c;  // payload only.  not header, pre_pad, post_pad, or footer.
    uint32_t len;
    uint32_t pre_pad, post_pad;
    uint64_t magic1;
    uint64_t magic2;

    static uint64_t make_magic(uint64_t seq, uint32_t len, uint64_t fsid) {
      return (fsid ^ seq ^ len);
    }
    bool check_magic(off64_t pos, uint64_t fsid) {
      return
    magic1 == (uint64_t)pos &&
    magic2 == (fsid ^ seq ^ len);
    }
  } __attribute__((__packed__, aligned(4)));

  bool journalq_empty() { return journalq.empty(); }

private:
  string fn;

  char *zero_buf;
  off64_t max_size;
  size_t block_size;
  bool directio, aio, force_aio;
  bool must_write_header;
  off64_t write_pos;      // byte where the next entry to be written will go
  off64_t read_pos;       //
  bool discard;	  //for block journal whether support discard

#ifdef HAVE_LIBAIO
  /// state associated with an in-flight aio request
  /// Protected by aio_lock
  struct aio_info {
    struct iocb iocb;
    bufferlist bl;
    struct iovec *iov;
    bool done;
    uint64_t off, len;    ///< these are for debug only
    uint64_t seq;         ///< seq number to complete on aio completion, if non-zero

    aio_info(bufferlist& b, uint64_t o, uint64_t s)
      : iov(NULL), done(false), off(o), len(b.length()), seq(s) {
      bl.claim(b);
      memset((void*)&iocb, 0, sizeof(iocb));
    }
    ~aio_info() {
      delete[] iov;
    }
  };
  Mutex aio_lock;
  Cond aio_cond;
  Cond write_finish_cond;
  io_context_t aio_ctx;
  list<aio_info> aio_queue;
  int aio_num, aio_bytes;
  uint64_t aio_write_queue_ops;
  uint64_t aio_write_queue_bytes;
  /// End protected by aio_lock
#endif

  uint64_t last_committed_seq;
  uint64_t journaled_since_start;

  /*
   * full states cycle at the beginnging of each commit epoch, when commit_start()
   * is called.
   *   FULL - we just filled up during this epoch.
   *   WAIT - we filled up last epoch; now we have to wait until everything during
   *          that epoch commits to the fs before we can start writing over it.
   *   NOTFULL - all good, journal away.
   */
  enum {
    FULL_NOTFULL = 0,
    FULL_FULL = 1,
    FULL_WAIT = 2,
  } full_state;

  int fd;

  // in journal
  deque<pair<uint64_t, off64_t> > journalq;  // track seq offsets, so we can trim later.
  uint64_t writing_seq;


  // throttle
  int set_throttle_params();
  const char** get_tracked_conf_keys() const override;
  void handle_conf_change(
    const struct md_config_t *conf,
    const std::set <std::string> &changed) override {
    for (const char **i = get_tracked_conf_keys();
	 *i;
	 ++i) {
      if (changed.count(string(*i))) {
	set_throttle_params();
	return;
      }
    }
  }

  void complete_write(uint64_t ops, uint64_t bytes);
  JournalThrottle throttle;

  // write thread
  Mutex write_lock;
  bool write_stop;
  bool aio_stop;

  Cond commit_cond;

  int _open(bool wr, bool create=false);
  int _open_block_device();
  void _close(int fd) const;
  void _check_disk_write_cache() const;
  int _open_file(int64_t oldsize, blksize_t blksize, bool create);
  int _dump(ostream& out, bool simple);
  void print_header(const header_t &hdr) const;
  int read_header(header_t *hdr) const;
  bufferptr prepare_header();
  void start_writer();
  void stop_writer();
  void write_thread_entry();

  void queue_completions_thru(uint64_t seq);

  int check_for_full(uint64_t seq, off64_t pos, off64_t size);
  int prepare_multi_write(bufferlist& bl, uint64_t& orig_ops, uint64_t& orig_bytee);
  int prepare_single_write(write_item &next_write, bufferlist& bl, off64_t& queue_pos,
    uint64_t& orig_ops, uint64_t& orig_bytes);
  void do_write(bufferlist& bl);

  void write_finish_thread_entry();
  void check_aio_completion();
  void do_aio_write(bufferlist& bl);
  int write_aio_bl(off64_t& pos, bufferlist& bl, uint64_t seq);


  void align_bl(off64_t pos, bufferlist& bl);
  int write_bl(off64_t& pos, bufferlist& bl);

  /// read len from journal starting at in_pos and wrapping up to len
  void wrap_read_bl(
    off64_t in_pos,   ///< [in] start position
    int64_t len,      ///< [in] length to read
    bufferlist* bl,   ///< [out] result
    off64_t *out_pos  ///< [out] next position to read, will be wrapped
    ) const;

  void do_discard(int64_t offset, int64_t end);

  class Writer : public Thread {
    FileJournal *journal;
  public:
    explicit Writer(FileJournal *fj) : journal(fj) {}
    void *entry() {
      journal->write_thread_entry();
      return 0;
    }
  } write_thread;

  class WriteFinisher : public Thread {
    FileJournal *journal;
  public:
    explicit WriteFinisher(FileJournal *fj) : journal(fj) {}
    void *entry() {
      journal->write_finish_thread_entry();
      return 0;
    }
  } write_finish_thread;

  off64_t get_top() const {
    return ROUND_UP_TO(sizeof(header), block_size);
  }

 public:
  FileJournal(uuid_d fsid, Finisher *fin, Cond *sync_cond, const char *f, bool dio=false, bool ai=true, bool faio=false) :
    Journal(fsid, fin, sync_cond),
    finisher_lock("FileJournal::finisher_lock", false, true, false, g_ceph_context),
    journaled_seq(0),
    plug_journal_completions(false),
    writeq_lock("FileJournal::writeq_lock", false, true, false, g_ceph_context),
    completions_lock(
      "FileJournal::completions_lock", false, true, false, g_ceph_context),
    fn(f),
    zero_buf(NULL),
    max_size(0), block_size(0),
    directio(dio), aio(ai), force_aio(faio),
    must_write_header(false),
    write_pos(0), read_pos(0),
    discard(false),
#ifdef HAVE_LIBAIO
    aio_lock("FileJournal::aio_lock"),
    aio_ctx(0),
    aio_num(0), aio_bytes(0),
    aio_write_queue_ops(0),
    aio_write_queue_bytes(0),
#endif
    last_committed_seq(0),
    journaled_since_start(0),
    full_state(FULL_NOTFULL),
    fd(-1),
    writing_seq(0),
    throttle(g_conf->filestore_caller_concurrency),
    write_lock("FileJournal::write_lock", false, true, false, g_ceph_context),
    write_stop(true),
    aio_stop(true),
    write_thread(this),
    write_finish_thread(this) {

      if (aio && !directio) {
        derr << "FileJournal::_open_any: aio not supported without directio; disabling aio" << dendl;
        aio = false;
      }
#ifndef HAVE_LIBAIO
      if (aio) {
        derr << "FileJournal::_open_any: libaio not compiled in; disabling aio" << dendl;
        aio = false;
      }
#endif

      g_conf->add_observer(this);
  }
  ~FileJournal() {
    assert(fd == -1);
    delete[] zero_buf;
    g_conf->remove_observer(this);
  }

  int check();
  int create();
  int open(uint64_t fs_op_seq);
  void close();
  int peek_fsid(uuid_d& fsid);

  int dump(ostream& out);
  int simple_dump(ostream& out);
  int _fdump(Formatter &f, bool simple);

  void flush();

  void reserve_throttle_and_backoff(uint64_t count);

  bool is_writeable() {
    return read_pos == 0;
  }
  int make_writeable();

  // writes
  void commit_start(uint64_t seq);
  void committed_thru(uint64_t seq);
  bool should_commit_now() {
    return full_state != FULL_NOTFULL && !write_stop;
  }

  void write_header_sync();

  void set_wait_on_full(bool b) { wait_on_full = b; }

  // reads

  /// Result code for read_entry
  enum read_entry_result {
    SUCCESS,
    FAILURE,
    MAYBE_CORRUPT
  };

  /**
   * read_entry
   *
   * Reads next entry starting at pos.  If the entry appears
   * clean, *bl will contain the payload, *seq will contain
   * the sequence number, and *out_pos will reflect the next
   * read position.  If the entry is invalid *ss will contain
   * debug text, while *seq, *out_pos, and *bl will be unchanged.
   *
   * If the entry suggests a corrupt log, *ss will contain debug
   * text, *out_pos will contain the next index to check.  If
   * we find an entry in this way that returns SUCCESS, the journal
   * is most likely corrupt.
   */
  read_entry_result do_read_entry(
    off64_t pos,          ///< [in] position to read
    off64_t *next_pos,    ///< [out] next position to read
    bufferlist* bl,       ///< [out] payload for successful read
    uint64_t *seq,        ///< [out] seq of successful read
    ostream *ss,          ///< [out] error output
    entry_header_t *h = 0 ///< [out] header
    ) const; ///< @return result code

  bool read_entry(
    bufferlist &bl,
    uint64_t &last_seq,
    bool *corrupt
    );

  bool read_entry(
    bufferlist &bl,
    uint64_t &last_seq) {
    return read_entry(bl, last_seq, 0);
  }

  // Debug/Testing
  void get_header(
    uint64_t wanted_seq,
    off64_t *_pos,
    entry_header_t *h);
  void corrupt(
    int wfd,
    off64_t corrupt_at);
  void corrupt_payload(
    int wfd,
    uint64_t seq);
  void corrupt_footer_magic(
    int wfd,
    uint64_t seq);
  void corrupt_header_magic(
    int wfd,
    uint64_t seq);
};

WRITE_CLASS_ENCODER(FileJournal::header_t)

#endif
