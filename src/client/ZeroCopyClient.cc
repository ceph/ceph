// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM Corporation
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "client/ZeroCopyClient.h"

#include <errno.h>

#include <cstdlib>
#include <mutex>
#include <vector>

#include "client/Client.h"
#include "client/ClientSnapRealm.h"
#include "client/Fh.h"
#include "client/Inode.h"
#include "common/Clock.h"
#include "common/Cond.h"
#include "common/Finisher.h"
#include "common/deleter.h"
#include "include/Context.h"
#include "include/ceph_assert.h"
#include "include/ceph_fs.h"
#include "osdc/Filer.h"
#include "osdc/Objecter.h"

#if defined(__linux__)
#include "client/FSCrypt.h"
#endif

namespace ceph {

void ZeroCopyClient::queue_user_cb(Client* clnt, IoCallback cb, int64_t result,
                                   bufferlist data)
{
  ceph_assert(clnt);
  clnt->objecter_finisher.queue(make_lambda_context(
      [cb = std::move(cb), result, data = std::move(data)](int) mutable {
        cb(IoResult{result, std::move(data)});
      }));
}

/**
 * Filer read completion. Objecter may invoke without client_lock; we take it,
 * finish short-read handling / optional FSCrypt decrypt, drop FILE_RD, unlock,
 * then run the user callback. Context::complete() deletes this after finish
 * returns (unless a short-read retry keeps the object alive).
 *
 * API payload is always plaintext: ciphertext from Filer is decrypted in place
 * via decrypt_bl before the user callback (copy into a new bufferlist).
 */
class C_ZC_Read : public Context {
public:
  Client* clnt;
  Fh* fh;
  InodeRef in;
  uint64_t off; ///< logical (plaintext) offset
  uint64_t len; ///< logical (plaintext) length requested
  bufferlist* bl;
  IoCallback cb;
  uint64_t pos;
  uint64_t left;
  uint64_t wanted;
  uint64_t read_bytes = 0;
  bufferlist tbl;
  Filer* filer;
  file_layout_t layout;
  snapid_t snapid;
  uint64_t truncate_size;
  uint32_t truncate_seq;
  bool fini = false; // like C_Read_Sync_NonBlocking: survive short-read retries
  utime_t start_time;
  uint64_t read_start = 0;
  uint64_t read_len = 0;
#if defined(__linux__)
  FSCryptFDataDencRef fscrypt_denc;
  bufferlist encbl; ///< accumulates ciphertext when fscrypt_denc is set
#endif

  C_ZC_Read(Client* clnt, Fh* fh, InodeRef in, uint64_t off, uint64_t len,
            bufferlist* bl, IoCallback cb, Filer* filer, file_layout_t layout,
            snapid_t snapid, uint64_t truncate_size, uint32_t truncate_seq) :
    clnt(clnt), fh(fh), in(std::move(in)), off(off), len(len), bl(bl),
    cb(std::move(cb)), pos(off), left(len), wanted(len), filer(filer),
    layout(std::move(layout)), snapid(snapid), truncate_size(truncate_size),
    truncate_seq(truncate_seq), start_time(mono_clock_now())
  {
    fh->get();
  }

  ~C_ZC_Read() override
  {
    delete bl;
    // finish() unlocks before the user callback, then complete() deletes us
    // without client_lock. fh->put() alone never deletes; need _put_fh under
    // lock or the Fh (and InodeRef) leaks and unmount hangs on inode_map.
    std::lock_guard lock(clnt->client_lock);
    clnt->_put_fh(fh);
  }

  void start()
  {
#if defined(__linux__)
    clnt->fscrypt->prepare_data_read(in->fscrypt_ctx,
                                     &in->fscrypt_key_validator, off, len,
                                     in->size, &read_start, &read_len,
                                     &fscrypt_denc);
    pos = read_start;
    left = read_len;
    wanted = read_len;
#else
    read_start = off;
    read_len = len;
    pos = off;
    left = len;
    wanted = len;
#endif
    retry();
  }

  void retry()
  {
    filer->read_trunc(in->ino, &layout, snapid, pos, left, &tbl, 0,
                      truncate_size, truncate_seq, this);
  }

  // Default Context::complete always deletes this; short-read retries re-submit
  // the same object to Filer, so only destroy when finish sets fini.
  void complete(int r) override
  {
    finish(r);
    if (fini) {
      delete this;
    }
  }

  void finish(int r) override
  {
    std::unique_lock lock(clnt->client_lock);

    auto effective_size = in->effective_size();
#if defined(__linux__)
    auto target_len = std::min(len,
                               effective_size > off ? effective_size - off : 0);
    bufferlist* pbl = (fscrypt_denc ? &encbl : bl);
#else
    bufferlist* pbl = bl;
#endif

    if (r == -ENOENT) {
      r = 0;
    } else if (r < 0) {
      goto done;
    }

    if (tbl.length()) {
      r = tbl.length();
      read_bytes += r;
      pos += r;
      left -= r;
      pbl->claim_append(tbl);
    }

    if (r >= 0 && static_cast<uint64_t>(r) < wanted) {
      // Short OSD read: zero up to known EOF like C_Read_Sync_NonBlocking,
      // then re-check size. Retry the remainder only if the file grew / we
      // have not reached EOF yet.
      if (pos < effective_size) {
        int64_t some = effective_size - pos;
        if (static_cast<uint64_t>(some) > left) {
          some = left;
        }
        auto z = buffer::ptr_node::create(some);
        z->zero();
        pbl->push_back(std::move(z));
        read_bytes += some;
        pos += some;
        left -= some;
        if (left == 0) {
          goto success;
        }
      }

      {
        int gr = clnt->_getattr(in, CEPH_STAT_CAP_SIZE, fh->actor_perms);
        if (gr < 0) {
          r = gr;
          goto done;
        }
      }

      effective_size = in->effective_size();
      if (pos >= effective_size || left == 0) {
        goto success;
      }

      wanted = left;
      truncate_size = in->truncate_size;
      truncate_seq = in->truncate_seq;
      // Keep client_lock across submit; fini stays false until final completion.
      retry();
      return;
    }

  success:
    if (r >= 0) {
#if defined(__linux__)
      if (fscrypt_denc) {
        if (read_len > target_len) {
          clnt->logger->inc(l_c_fscrypt_rd_amp, read_len - target_len);
        }
        std::vector<FSCryptFDataDenc::Segment> holes;
        auto dec_start = mono_clock_now();
        r = fscrypt_denc->decrypt_bl(off, target_len, read_start, holes, pbl);
        clnt->logger->tinc(l_c_fscrypt_dec_lat, mono_clock_now() - dec_start);
        if (r < 0) {
          goto done;
        }
        bl->claim_append(*pbl);
        clnt->logger->tinc(l_c_fscrypt_rd_lat, mono_clock_now() - start_time);
      }
#endif
      // Match C_Read_Sync_NonBlocking: full physical read → report logical len.
      if (read_len == read_bytes) {
        r = len;
      } else {
        r = read_bytes;
      }
    }

  done:
    fini = true;
    clnt->put_cap_ref(in.get(), CEPH_CAP_FILE_RD);
    clnt->logger->inc(l_c_aio_completions);
    clnt->dec_aio_in_flight();
    IoCallback user_cb = std::move(cb);
    bufferlist out;
    if (r >= 0) {
      out = std::move(*bl);
    }
    lock.unlock();
    user_cb(IoResult{r, std::move(out)});
  }
};

/** Optional post-write fsync; runs under client_lock. */
class C_ZC_Fsync : public Context {
public:
  Client* clnt;
  InodeRef in;
  IoCallback cb;
  int64_t io_result;

  C_ZC_Fsync(Client* clnt, InodeRef in, IoCallback cb, int64_t io_result) :
    clnt(clnt), in(std::move(in)), cb(std::move(cb)), io_result(io_result)
  {}

  void finish(int r) override
  {
    ceph_assert(ceph_mutex_is_locked_by_me(clnt->client_lock));
    clnt->put_cap_ref(in.get(), CEPH_CAP_FILE_WR);
    // Match C_Write_Finisher: count the op complete only after optional fsync.
    clnt->logger->inc(l_c_aio_completions);
    clnt->dec_aio_in_flight();
    ZeroCopyClient::queue_user_cb(clnt, std::move(cb), r < 0 ? r : io_result,
                                  {});
  }
};

/**
 * Filer write completion under client_lock (via C_Lock_Client_Finisher).
 * Updates size, drops BUFFER/WR (WR after optional fsync), then user cb.
 * Holds Client* only — never ZeroCopyClient* (stack wrappers must not be pinned).
 *
 * Payload lifetime: WriteEncMgr holds plaintext (and ciphertext after encrypt)
 * until Objecter completion; do not keep a separate held_bl.
 */
class C_ZC_Write : public Context {
public:
  Client* clnt;
  Fh* fh;
  InodeRef in;
  utime_t start;
  uint64_t req_ofs; ///< logical (plaintext) offset
  uint64_t req_size; ///< logical (plaintext) length
  uint64_t fpos; ///< 0 = do not update fh pos
  bool do_fsync;
  bool syncdataonly;
  IoCallback cb;
  ceph::ref_t<Client::WriteEncMgr> enc_mgr;

  C_ZC_Write(Client* clnt, Fh* fh, InodeRef in, utime_t start, uint64_t req_ofs,
             uint64_t req_size, uint64_t fpos, bool do_fsync, bool syncdataonly,
             IoCallback cb, ceph::ref_t<Client::WriteEncMgr> enc_mgr) :
    clnt(clnt), fh(fh), in(std::move(in)), start(start), req_ofs(req_ofs),
    req_size(req_size), fpos(fpos), do_fsync(do_fsync),
    syncdataonly(syncdataonly), cb(std::move(cb)), enc_mgr(std::move(enc_mgr))
  {
    fh->get();
  }

  ~C_ZC_Write() override
  {
    // Destroyed under client_lock (C_Lock_Client_Finisher → complete). Do not
    // re-lock — match C_Readahead. fh->put() alone never deletes.
    ceph_assert(ceph_mutex_is_locked_by_me(clnt->client_lock));
    clnt->_put_fh(fh);
  }

  void finish(int r) override
  {
    ceph_assert(ceph_mutex_is_locked_by_me(clnt->client_lock));

    clnt->put_cap_ref(in.get(), CEPH_CAP_FILE_BUFFER);

    const bool encrypted = enc_mgr->encrypted();
    const int64_t phys_ofs = enc_mgr->get_ofs();
    const uint64_t phys_size = enc_mgr->get_size();

    int64_t result = r;
    if (r >= 0) {
      result = clnt->_write_success(fh, start, fpos, req_ofs, req_size,
                                    phys_ofs, phys_size, in.get(), encrypted);
      if (encrypted) {
        clnt->logger->tinc(l_c_fscrypt_wr_lat, mono_clock_now() - start);
      }
    }

    if (r >= 0 && do_fsync) {
      // Match C_Write_Finisher: kick C_nonblocking_fsync_state under the
      // client_lock we already hold. Do not call nonblocking_fsync() — that
      // entry point takes client_lock again and would assert. aio_in_flight
      // stays up until C_ZC_Fsync::finish (same as C_Write_Finisher).
      auto* fsync_ctx = new C_ZC_Fsync(clnt, in, cb, result);
      auto* state = new Client::C_nonblocking_fsync_state(clnt, in.get(),
                                                          syncdataonly,
                                                          fsync_ctx);
      state->advance();
      return;
    }

    clnt->put_cap_ref(in.get(), CEPH_CAP_FILE_WR);
    clnt->logger->inc(l_c_aio_completions);
    clnt->dec_aio_in_flight();
    ZeroCopyClient::queue_user_cb(clnt, std::move(cb), result, {});
  }
};

ZeroCopyClient::ZeroCopyClient(Client* client) : client(client) {}

bufferlist ZeroCopyClient::alloc_buffers(size_t len)
{
  bufferlist bl;
  if (len == 0) {
    return bl;
  }
  // One page-aligned raw of exact @p len: base is CEPH_PAGE_SIZE-aligned for
  // readv/DMA; length matches the WRITE so to_iovec yields a single segment
  // (FSAL IOV_MAX-friendly) and ObjectCacher substr_of can share the same raw.
  bl.push_back(bufferptr(buffer::create_page_aligned(len)));
  ceph_assert(bl.length() == len);
  return bl;
}

bufferlist ZeroCopyClient::wrap_static(char* buf, size_t len)
{
  if (len == 0 || buf == nullptr) {
    return bufferlist();
  }
  return bufferlist::static_from_mem(buf, len);
}

bufferlist ZeroCopyClient::wrap_static(const struct iovec* iov, int iovcnt)
{
  bufferlist bl;
  if (iov == nullptr || iovcnt <= 0) {
    return bl;
  }
  for (int i = 0; i < iovcnt; ++i) {
    if (iov[i].iov_base == nullptr || iov[i].iov_len == 0) {
      continue;
    }
    bl.push_back(buffer::create_static(iov[i].iov_len,
                                       static_cast<char*>(iov[i].iov_base)));
  }
  return bl;
}

namespace {

::deleter make_claim_deleter(ZeroCopyClient::ClaimDeleter del, void* priv,
                             char* buf, size_t len)
{
  if (del == nullptr) {
    return make_free_deleter(buf);
  }
  // Capture buf/len/priv by value; del runs when raw_claim_buffer is destroyed.
  return make_deleter([del, priv, buf, len]() { del(buf, len, priv); });
}

} // namespace

bufferlist ZeroCopyClient::claim(char* buf, size_t len, ClaimDeleter del,
                                 void* priv)
{
  bufferlist bl;
  if (len == 0 || buf == nullptr) {
    return bl;
  }
  bl.push_back(buffer::claim_buffer(len, buf,
                                    make_claim_deleter(del, priv, buf, len)));
  return bl;
}

bufferlist ZeroCopyClient::claim_iov(const struct iovec* iov, int iovcnt,
                                     ClaimDeleter del, void* priv)
{
  bufferlist bl;
  if (iov == nullptr || iovcnt <= 0) {
    return bl;
  }
  for (int i = 0; i < iovcnt; ++i) {
    if (iov[i].iov_base == nullptr || iov[i].iov_len == 0) {
      continue;
    }
    auto* base = static_cast<char*>(iov[i].iov_base);
    const size_t len = iov[i].iov_len;
    bl.push_back(buffer::claim_buffer(
        len, base, make_claim_deleter(del, priv, base, len)));
  }
  return bl;
}

int ZeroCopyClient::bufferlist_to_iovec(const bufferlist& bl, struct iovec* iov,
                                        int max_iov)
{
  if (iov == nullptr && bl.get_num_buffers() > 0) {
    return -EINVAL;
  }
  if (max_iov < 0) {
    return -EINVAL;
  }

  int n = 0;
  for (const auto& p : bl.buffers()) {
    if (p.length() == 0) {
      continue;
    }
    if (n >= max_iov) {
      return -ERANGE;
    }
    iov[n].iov_base = const_cast<char*>(p.c_str());
    iov[n].iov_len = p.length();
    ++n;
  }
  return n;
}

int ZeroCopyClient::async_read(Fh* fh, uint64_t off, uint64_t len, IoCallback cb)
{
  if (!client || !cb) {
    return -EINVAL;
  }

  Client::RWRef_t mref_reader(client->mount_state, Client::CLIENT_MOUNTING);
  if (!mref_reader.is_state_satisfied()) {
    queue_user_cb(client, std::move(cb), -ENOTCONN, {});
    return 0;
  }

  std::unique_lock lock(client->client_lock);

  if (fh == nullptr || !client->_ll_fh_exists(fh)) {
    lock.unlock();
    queue_user_cb(client, std::move(cb), -EBADF, {});
    return 0;
  }
  if ((fh->mode & CEPH_FILE_MODE_RD) == 0) {
    lock.unlock();
    queue_user_cb(client, std::move(cb), -EBADF, {});
    return 0;
  }
  if (len == 0) {
    lock.unlock();
    queue_user_cb(client, std::move(cb), 0, {});
    return 0;
  }

  Inode* in = fh->inode.get();
  int have = 0;
  // Prefer CACHE for issue; still always use Filer (no ObjectCacher).
  int r = client->get_caps(fh, CEPH_CAP_FILE_RD, CEPH_CAP_FILE_CACHE, &have, -1);
  if (r < 0) {
    lock.unlock();
    queue_user_cb(client, std::move(cb), r, {});
    return 0;
  }

  auto effective_size = in->effective_size();
  if (off >= effective_size) {
    client->put_cap_ref(in, CEPH_CAP_FILE_RD);
    lock.unlock();
    queue_user_cb(client, std::move(cb), 0, {});
    return 0;
  }
  if (off + len > effective_size) {
    len = effective_size - off;
  }

  auto* bl = new bufferlist();
  auto* read_ctx = new C_ZC_Read(client, fh, InodeRef(in), off, len, bl,
                                 std::move(cb), client->filer.get(), in->layout,
                                 in->snapid, in->truncate_size,
                                 in->truncate_seq);

  // Match _read / C_Read_Finisher: count from Filer submit through final
  // completion (short-read retries stay a single in-flight op).
  client->logger->inc(l_c_aio_ops);
  client->inc_aio_in_flight();

  lock.unlock();
  read_ctx->start();
  return 0;
}

int ZeroCopyClient::async_write(Fh* fh, uint64_t off, bufferlist data,
                                IoCallback cb, bool fsync, bool syncdataonly)
{
  if (!client || !cb) {
    return -EINVAL;
  }

  const uint64_t size = data.length();
  if (size < 1) {
    return -EINVAL;
  }

  Client::RWRef_t mref_reader(client->mount_state, Client::CLIENT_MOUNTING);
  if (!mref_reader.is_state_satisfied()) {
    queue_user_cb(client, std::move(cb), -ENOTCONN, {});
    return 0;
  }

  std::unique_lock lock(client->client_lock);

  if (fh == nullptr || !client->_ll_fh_exists(fh)) {
    lock.unlock();
    queue_user_cb(client, std::move(cb), -EBADF, {});
    return 0;
  }
  if ((fh->mode & CEPH_FILE_MODE_WR) == 0) {
    lock.unlock();
    queue_user_cb(client, std::move(cb), -EBADF, {});
    return 0;
  }

  Inode* in = fh->inode.get();
  if (in->snapid != CEPH_NOSNAP) {
    lock.unlock();
    queue_user_cb(client, std::move(cb), -EROFS, {});
    return 0;
  }

  uint64_t offset = off;
  uint64_t fpos = 0;
  if (static_cast<int64_t>(off) < 0) {
    client->lock_fh_pos(fh);
    if (fh->flags & O_APPEND) {
      auto lr = client->_lseek(fh, 0, SEEK_END);
      if (lr < 0) {
        client->unlock_fh_pos(fh);
        lock.unlock();
        queue_user_cb(client, std::move(cb), lr, {});
        return 0;
      }
    }
    offset = fh->pos;
    fpos = offset + size;
    client->unlock_fh_pos(fh);
  }

  if ((offset + size) > client->mdsmap->get_max_filesize() &&
      (offset + size) > in->size) {
    lock.unlock();
    queue_user_cb(client, std::move(cb), -EFBIG, {});
    return 0;
  }
  if (client->objecter->osdmap_pool_full(in->layout.pool_id)) {
    lock.unlock();
    queue_user_cb(client, std::move(cb), -ENOSPC, {});
    return 0;
  }

  uint64_t endoff = offset + size;
  if (endoff > in->size &&
      client->is_quota_bytes_exceeded(in, endoff - in->size, fh->actor_perms)) {
    lock.unlock();
    queue_user_cb(client, std::move(cb), -EDQUOT, {});
    return 0;
  }

  utime_t start = mono_clock_now();

  int have = 0;
  int r = client->get_caps(fh, CEPH_CAP_FILE_WR | CEPH_CAP_AUTH_SHARED,
                           CEPH_CAP_FILE_BUFFER, &have, endoff);
  if (r < 0) {
    lock.unlock();
    queue_user_cb(client, std::move(cb), r, {});
    return 0;
  }
  client->put_cap_ref(in, CEPH_CAP_AUTH_SHARED);

  r = client->clear_suid_sgid(in, fh->actor_perms);
  if (r < 0) {
    client->put_cap_ref(in, CEPH_CAP_FILE_WR);
    lock.unlock();
    queue_user_cb(client, std::move(cb), r, {});
    return 0;
  }

  if (!in->snaprealm) {
    client->put_cap_ref(in, CEPH_CAP_FILE_WR);
    lock.unlock();
    queue_user_cb(client, std::move(cb), -EINVAL, {});
    return 0;
  }

  // Filer-only sink today (ObjectCacher routing is separate). WriteEncMgr
  // performs optional FSCrypt RMW+encrypt (copy into ciphertext bl) then
  // filer->write_trunc. BUFFER cap is taken inside do_write.
  auto enc_mgr = ceph::make_ref<Client::WriteEncMgr_NotBuffered>(
      client, fh, offset, size, data, true /* async */);
  r = enc_mgr->init();
  if (r < 0) {
    client->put_cap_ref(in, CEPH_CAP_FILE_WR);
    lock.unlock();
    queue_user_cb(client, std::move(cb), r, {});
    return 0;
  }

  auto* write_ctx = new C_ZC_Write(client, fh, InodeRef(in), start, offset,
                                   size, fpos, fsync, syncdataonly,
                                   std::move(cb), enc_mgr);

  Context* filer_onfinish =
      new C_OnFinisher(new Client::C_Lock_Client_Finisher(client, write_ctx),
                       &client->objecter_finisher);

  // Match _write / C_Write_Finisher (held through optional fsync).
  client->logger->inc(l_c_aio_ops);
  client->inc_aio_in_flight();

  // read_modify_write requires client_lock for the encrypted RMW path.
  // Ownership of filer_onfinish transfers into WriteEncMgr's completion
  // lambda (and then Objecter). Match Client::_write Filer async: do not
  // specially unwind on rare early RMW errors.
  (void)enc_mgr->read_modify_write(filer_onfinish);
  lock.unlock();
  return 0;
}

int64_t ZeroCopyClient::read(Fh* fh, uint64_t off, uint64_t len, bufferlist* out)
{
  if (!out) {
    return -EINVAL;
  }
  C_SaferCond cond;
  int r = async_read(fh, off, len, [&cond, out](IoResult&& res) {
    if (res.result >= 0) {
      *out = std::move(res.data);
    }
    cond.complete(res.result);
  });
  if (r < 0) {
    return r;
  }
  return cond.wait();
}

int64_t ZeroCopyClient::write(Fh* fh, uint64_t off, bufferlist data, bool fsync)
{
  C_SaferCond cond;
  int r = async_write(
      fh, off, std::move(data),
      [&cond](IoResult&& res) { cond.complete(res.result); }, fsync, false);
  if (r < 0) {
    return r;
  }
  return cond.wait();
}

} // namespace ceph
