// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2026 IBM
 *
 * See file COPYING for licensing information.
 */

#include "rgw_output_stream.h"

#include <iterator>
#include <utility>

#include <arrow/util/io_util.h>

#include "rgw_common.h"
#include "rgw_hex.h"
#include "rgw_req_context.h"

#define dout_subsys ceph_subsys_rgw

namespace {

// Maps a negative errno from the SAL to an IOError carrying an errno detail,
// the same convention Arrow's own filesystems use: ToString() shows the
// strerror text, and arrow::internal::ErrnoFromStatus() recovers the errno
// (ENOENT for a missing object).
arrow::Status errno_status(const char* what, const std::string& name, int r)
{
  return arrow::internal::IOErrorFromErrno(-r, "RGWOutputStream: ", what, " ", name);
}

} // anonymous namespace

RGWOutputStream::RGWOutputStream(const DoutPrefixProvider* dpp,
                                 optional_yield y,
                                 std::unique_ptr<rgw::sal::Object> obj,
                                 rgw::sal::Attrs attrs, std::string unique_tag,
                                 int64_t chunk_size)
  : dpp_(dpp), y_(y), chunk_size_(chunk_size), obj_(std::move(obj)),
    attrs_(std::move(attrs)), unique_tag_(std::move(unique_tag))
{
  set_mode(arrow::io::FileMode::WRITE);
  const rgw::sal::Bucket* b = obj_->get_bucket();
  name_ = (b ? b->get_name() : std::string("?")) + "/" + obj_->get_name();
  hash_.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
}

RGWOutputStream::~RGWOutputStream()
{
  if (!closed_) {
    ldpp_dout(dpp_, 5) << "RGWOutputStream: " << name_
                       << " destroyed without Close(), object discarded"
                       << dendl;
    closed_ = true;
  }
}

arrow::Result<std::shared_ptr<RGWOutputStream>> RGWOutputStream::Open(
    const DoutPrefixProvider* dpp, optional_yield y, rgw::sal::Driver* driver,
    std::unique_ptr<rgw::sal::Object> obj, rgw::sal::Attrs attrs,
    std::string unique_tag, int64_t chunk_size)
{
  if (!driver) {
    return arrow::Status::Invalid("RGWOutputStream: null driver");
  }
  if (!obj) {
    return arrow::Status::Invalid("RGWOutputStream: null object");
  }
  if (!obj->get_bucket()) {
    return arrow::Status::Invalid("RGWOutputStream: object ", obj->get_name(),
                                  " has no bucket");
  }
  if (chunk_size <= 0) {
    return arrow::Status::Invalid("RGWOutputStream: invalid chunk size ",
                                  chunk_size);
  }
  // not make_shared: the constructor is private
  std::shared_ptr<RGWOutputStream> stream(new RGWOutputStream(
      dpp, y, std::move(obj), std::move(attrs), std::move(unique_tag),
      chunk_size));
  ARROW_RETURN_NOT_OK(stream->prepare(driver));
  return stream;
}

arrow::Result<std::shared_ptr<RGWOutputStream>> RGWOutputStream::Open(
    const DoutPrefixProvider* dpp, optional_yield y, rgw::sal::Driver* driver,
    rgw::sal::Bucket* bucket, const rgw_obj_key& key, rgw::sal::Attrs attrs,
    std::string unique_tag, int64_t chunk_size)
{
  if (!bucket) {
    return arrow::Status::Invalid("RGWOutputStream: null bucket");
  }
  std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(key);
  if (!obj) {
    // the SAL factories carry no error code: a null is all they can report
    return arrow::Status::IOError(
        "RGWOutputStream: driver returned no object for ", key.name);
  }
  return Open(dpp, y, driver, std::move(obj), std::move(attrs),
              std::move(unique_tag), chunk_size);
}

arrow::Status RGWOutputStream::prepare(rgw::sal::Driver* driver)
{
  rgw::sal::Bucket* bucket = obj_->get_bucket();
  const ACLOwner owner = bucket->get_acl().get_owner();
  const rgw_placement_rule& placement = bucket->get_placement_rule();
  if (unique_tag_.empty()) {
    // what a request would use: its request id
    unique_tag_ = driver->zone_unique_id(driver->get_new_req_id());
  }

  writer_ = driver->get_atomic_writer(dpp_, y_, obj_.get(), owner, &placement,
                                      /*olh_epoch=*/0, unique_tag_);
  if (!writer_) {
    return arrow::Status::IOError(
        "RGWOutputStream: driver returned no writer for ", name_);
  }
  int r = writer_->prepare(y_);
  if (r < 0) {
    writer_.reset();
    return errno_status("failed to prepare write of", name_, r);
  }
  ldpp_dout(dpp_, 20) << "RGWOutputStream: opened " << name_
                      << " tag=" << unique_tag_
                      << " chunk_size=" << chunk_size_ << dendl;
  return arrow::Status::OK();
}

arrow::Status RGWOutputStream::check_open() const
{
  if (closed_) {
    return arrow::Status::Invalid("RGWOutputStream: ", name_, " is closed");
  }
  return arrow::Status::OK();
}

arrow::Status RGWOutputStream::flush_pending(bool final)
{
  if (pending_.length() > 0) {
    const int64_t len = pending_.length();
    ldpp_dout(dpp_, 20) << "RGWOutputStream: write " << name_ << " ["
                        << flushed_ << ", " << flushed_ + len - 1 << "]"
                        << dendl;
    int r = writer_->process(std::move(pending_), flushed_);
    pending_.clear();
    if (r < 0) {
      return errno_status("failed to write", name_, r);
    }
    flushed_ += len;
  }
  if (final) {
    // an empty buffer tells the writer that this was the last one
    int r = writer_->process(ceph::bufferlist(), flushed_);
    if (r < 0) {
      return errno_status("failed to flush", name_, r);
    }
  }
  return arrow::Status::OK();
}

// ---- Writable ----

arrow::Status RGWOutputStream::Write(const void* data, int64_t nbytes)
{
  ARROW_RETURN_NOT_OK(check_open());
  if (nbytes < 0) {
    return arrow::Status::Invalid("RGWOutputStream: invalid write of ", nbytes,
                                  " bytes");
  }
  if (nbytes == 0) {
    return arrow::Status::OK();
  }
  if (!data) {
    return arrow::Status::Invalid("RGWOutputStream: null input buffer");
  }
  // the caller's buffer is only valid for the duration of the call: copy it
  const char* p = static_cast<const char*>(data);
  hash_.Update(reinterpret_cast<const unsigned char*>(p), nbytes);
  pending_.append(p, nbytes);
  pos_ += nbytes;
  if (static_cast<int64_t>(pending_.length()) >= chunk_size_) {
    return flush_pending(/*final=*/false);
  }
  return arrow::Status::OK();
}

arrow::Status RGWOutputStream::Flush()
{
  ARROW_RETURN_NOT_OK(check_open());
  return flush_pending(/*final=*/false);
}

// ---- FileInterface ----

arrow::Result<int64_t> RGWOutputStream::Tell() const
{
  ARROW_RETURN_NOT_OK(check_open());
  return pos_;
}

bool RGWOutputStream::closed() const
{
  return closed_;
}

arrow::Status RGWOutputStream::Abort()
{
  if (!closed_) {
    ldpp_dout(dpp_, 20) << "RGWOutputStream: aborted " << name_ << " after "
                        << pos_ << " bytes, object discarded" << dendl;
    pending_.clear();
    closed_ = true;
  }
  return arrow::Status::OK();
}

arrow::Status RGWOutputStream::Close()
{
  if (closed_) {
    return arrow::Status::OK();
  }
  // whatever happens below, the stream is unusable afterwards
  closed_ = true;

  ARROW_RETURN_NOT_OK(flush_pending(/*final=*/true));

  unsigned char digest[CEPH_CRYPTO_MD5_DIGESTSIZE];
  hash_.Final(digest);
  std::string etag;
  buf_to_hex(digest, std::back_inserter(etag));

  ceph::bufferlist etag_bl;
  etag_bl.append(etag);
  attrs_[RGW_ATTR_ETAG] = std::move(etag_bl);

  ceph::real_time mtime;
  const req_context rctx{dpp_, y_, nullptr};
  int r = writer_->complete(pos_, etag, &mtime, ceph::real_time(), attrs_,
                            std::nullopt, ceph::real_time(),
                            /*if_match=*/nullptr, /*if_nomatch=*/nullptr,
                            /*user_data=*/nullptr, /*zones_trace=*/nullptr,
                            /*canceled=*/nullptr, rctx, rgw::sal::FLAG_LOG_OP);
  if (r < 0) {
    return errno_status("failed to complete write of", name_, r);
  }
  etag_ = std::move(etag);
  ldpp_dout(dpp_, 20) << "RGWOutputStream: closed " << name_ << " size=" << pos_
                      << " etag=" << etag_ << dendl;
  return arrow::Status::OK();
}
