// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2026 IBM
 *
 * See file COPYING for licensing information.
 */

#include "rgw_random_access_file.h"

#include <algorithm>
#include <utility>
#include <vector>

#include <arrow/buffer.h>
#include <arrow/util/io_util.h>
#include <arrow/util/key_value_metadata.h>

#include "common/iso_8601.h"
#include "rgw_common.h"

#define dout_subsys ceph_subsys_rgw

namespace {

// RGWGetDataCB that copies the chunks delivered by ReadOp::iterate() straight
// into a caller-provided buffer.  The rados driver hands chunks over in
// offset order, each covering [bl_ofs, bl_ofs + bl_len) of bl; segmented
// bufferlists are copied segment by segment, so there is no intermediate
// contiguous copy.
class CopyToBufferCB : public RGWGetDataCB {
public:
  CopyToBufferCB(char* out, int64_t capacity) : out_(out), capacity_(capacity) {}

  int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) override {
    if (bl_len < 0 || bl_ofs < 0 ||
        static_cast<uint64_t>(bl_ofs) + bl_len > bl.length()) {
      return -EINVAL;
    }
    if (bl_len > capacity_ - written_) {
      // the driver delivered more than the range we asked for
      return -EOVERFLOW;
    }
    auto it = bl.cbegin();
    it += bl_ofs;
    it.copy(bl_len, out_ + written_);
    written_ += bl_len;
    return 0;
  }

  int64_t written() const { return written_; }

private:
  char* const out_;
  const int64_t capacity_;
  int64_t written_ = 0;
};

// Maps a negative errno from the SAL to an IOError carrying an errno detail,
// the same convention Arrow's own filesystems use: ToString() shows the
// strerror text, and arrow::internal::ErrnoFromStatus() recovers the errno
// (ENOENT for a missing object).
arrow::Status errno_status(const char* what, const std::string& name, int r)
{
  return arrow::internal::IOErrorFromErrno(-r, "RGWRandomAccessFile: ", what, " ", name);
}

} // anonymous namespace

RGWRandomAccessFile::RGWRandomAccessFile(const DoutPrefixProvider* dpp,
                                         optional_yield y,
                                         std::unique_ptr<rgw::sal::Object> obj)
  : dpp_(dpp), y_(y), obj_(std::move(obj))
{
  const rgw::sal::Bucket* b = obj_->get_bucket();
  name_ = (b ? b->get_name() : std::string("?")) + "/" + obj_->get_name();
  if (!obj_->get_instance().empty()) {
    name_ += "@" + obj_->get_instance();
  }
}

RGWRandomAccessFile::~RGWRandomAccessFile() = default;

arrow::Result<std::shared_ptr<RGWRandomAccessFile>> RGWRandomAccessFile::Open(
    const DoutPrefixProvider* dpp, optional_yield y,
    std::unique_ptr<rgw::sal::Object> obj)
{
  if (!obj) {
    return arrow::Status::Invalid("RGWRandomAccessFile: null object");
  }
  // not make_shared: the constructor is private
  std::shared_ptr<RGWRandomAccessFile> file(
      new RGWRandomAccessFile(dpp, y, std::move(obj)));
  ARROW_RETURN_NOT_OK(file->load_object_state());
  return file;
}

arrow::Result<std::shared_ptr<RGWRandomAccessFile>> RGWRandomAccessFile::Open(
    const DoutPrefixProvider* dpp, optional_yield y,
    rgw::sal::Bucket* bucket, const rgw_obj_key& key)
{
  if (!bucket) {
    return arrow::Status::Invalid("RGWRandomAccessFile: null bucket");
  }
  std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(key);
  if (!obj) {
    // the SAL factories carry no error code: a null is all they can report
    return arrow::Status::IOError(
        "RGWRandomAccessFile: driver returned no object for ", key.name);
  }
  return Open(dpp, y, std::move(obj));
}

arrow::Status RGWRandomAccessFile::load_object_state()
{
  int r = obj_->load_obj_state(dpp_, y_);
  if (r < 0) {
    return errno_status("failed to load object state for", name_, r);
  }
  if (!obj_->exists()) {
    return errno_status("failed to open", name_, -ENOENT);
  }
  size_ = static_cast<int64_t>(obj_->get_size());
  attrs_ = obj_->get_attrs();
  build_metadata();
  ldpp_dout(dpp_, 20) << "RGWRandomAccessFile: opened " << name_
                      << " size=" << size_ << dendl;
  return arrow::Status::OK();
}

void RGWRandomAccessFile::build_metadata()
{
  // expose the object the way an S3 HEAD would: standard headers plus the
  // user metadata, with the internal attribute prefix stripped
  std::vector<std::string> keys;
  std::vector<std::string> values;
  keys.emplace_back("Content-Length");
  values.emplace_back(std::to_string(size_));
  keys.emplace_back("Last-Modified");
  values.emplace_back(ceph::to_iso_8601(obj_->get_mtime()));
  for (const auto& [name, bl] : attrs_) {
    if (name == RGW_ATTR_ETAG) {
      keys.emplace_back("ETag");
      values.emplace_back(rgw_bl_str(bl));
    } else if (name == RGW_ATTR_CONTENT_TYPE) {
      keys.emplace_back("Content-Type");
      values.emplace_back(rgw_bl_str(bl));
    } else if (name.starts_with(RGW_ATTR_META_PREFIX)) {
      keys.emplace_back(RGW_AMZ_META_PREFIX +
                        name.substr(sizeof(RGW_ATTR_META_PREFIX) - 1));
      values.emplace_back(rgw_bl_str(bl));
    }
  }
  metadata_ = std::make_shared<const arrow::KeyValueMetadata>(
      std::move(keys), std::move(values));
}

arrow::Status RGWRandomAccessFile::check_open() const
{
  if (closed_.load()) {
    return arrow::Status::Invalid("RGWRandomAccessFile: ", name_, " is closed");
  }
  return arrow::Status::OK();
}

// ---- FileInterface ----

arrow::Status RGWRandomAccessFile::Close()
{
  if (!closed_.exchange(true)) {
    ldpp_dout(dpp_, 20) << "RGWRandomAccessFile: closed " << name_ << dendl;
  }
  return arrow::Status::OK();
}

arrow::Result<int64_t> RGWRandomAccessFile::Tell() const
{
  ARROW_RETURN_NOT_OK(check_open());
  return pos_.load();
}

bool RGWRandomAccessFile::closed() const
{
  return closed_.load();
}

// ---- Seekable ----

arrow::Status RGWRandomAccessFile::Seek(int64_t position)
{
  ARROW_RETURN_NOT_OK(check_open());
  if (position < 0 || position > size_) {
    return arrow::Status::Invalid("RGWRandomAccessFile: seek to ", position,
                                  " is out of bounds for ", name_,
                                  " of size ", size_);
  }
  pos_.store(position);
  return arrow::Status::OK();
}

// ---- RandomAccessFile ----

arrow::Result<int64_t> RGWRandomAccessFile::GetSize()
{
  ARROW_RETURN_NOT_OK(check_open());
  return size_;
}

arrow::Result<int64_t> RGWRandomAccessFile::ReadAt(int64_t position,
                                                   int64_t nbytes, void* out)
{
  ARROW_RETURN_NOT_OK(check_open());
  if (position < 0 || nbytes < 0) {
    return arrow::Status::Invalid("RGWRandomAccessFile: invalid read of ",
                                  nbytes, " bytes at ", position);
  }
  if (nbytes == 0 || position >= size_) {
    return 0;
  }
  if (!out) {
    return arrow::Status::Invalid("RGWRandomAccessFile: null output buffer");
  }
  // reads past the end are truncated, as the Arrow contract allows
  const int64_t len = std::min(nbytes, size_ - position);
  const int64_t end = position + len - 1;  // iterate() takes an inclusive end

  ldpp_dout(dpp_, 20) << "RGWRandomAccessFile: read " << name_ << " ["
                      << position << ", " << end << "]" << dendl;

  // a ReadOp per call: nothing is shared between concurrent readers
  std::unique_ptr<rgw::sal::Object::ReadOp> read_op = obj_->get_read_op();
  if (!read_op) {
    return arrow::Status::IOError(
        "RGWRandomAccessFile: driver returned no read op for ", name_);
  }
  int r = read_op->prepare(y_, dpp_);
  if (r < 0) {
    return errno_status("failed to prepare read of", name_, r);
  }
  CopyToBufferCB cb(static_cast<char*>(out), len);
  r = read_op->iterate(dpp_, position, end, &cb, y_);
  if (r < 0) {
    return errno_status("failed to read", name_, r);
  }
  if (cb.written() != len) {
    return arrow::Status::IOError("RGWRandomAccessFile: short read of ", name_,
                                  " at ", position, ": got ", cb.written(),
                                  " of ", len, " bytes");
  }
  return len;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> RGWRandomAccessFile::ReadAt(
    int64_t position, int64_t nbytes)
{
  ARROW_RETURN_NOT_OK(check_open());
  if (position < 0 || nbytes < 0) {
    return arrow::Status::Invalid("RGWRandomAccessFile: invalid read of ",
                                  nbytes, " bytes at ", position);
  }
  // size the allocation by what can actually be read, not by the request
  const int64_t len = position >= size_ ? 0 : std::min(nbytes, size_ - position);
  ARROW_ASSIGN_OR_RAISE(std::unique_ptr<arrow::ResizableBuffer> buf,
                        arrow::AllocateResizableBuffer(len));
  ARROW_ASSIGN_OR_RAISE(const int64_t n,
                        ReadAt(position, len, buf->mutable_data()));
  if (n != len) {
    ARROW_RETURN_NOT_OK(buf->Resize(n, /*shrink_to_fit=*/true));
  }
  return std::shared_ptr<arrow::Buffer>(std::move(buf));
}

// ---- Readable / InputStream (sequential) ----

arrow::Result<int64_t> RGWRandomAccessFile::Read(int64_t nbytes, void* out)
{
  ARROW_ASSIGN_OR_RAISE(const int64_t n, ReadAt(pos_.load(), nbytes, out));
  pos_ += n;
  return n;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> RGWRandomAccessFile::Read(
    int64_t nbytes)
{
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> buf,
                        ReadAt(pos_.load(), nbytes));
  pos_ += buf->size();
  return buf;
}

arrow::Result<std::string_view> RGWRandomAccessFile::Peek(int64_t nbytes)
{
  ARROW_RETURN_NOT_OK(check_open());
  if (nbytes < 0) {
    return arrow::Status::Invalid("RGWRandomAccessFile: invalid peek of ",
                                  nbytes, " bytes");
  }
  const int64_t pos = pos_.load();
  const int64_t len = pos >= size_ ? 0 : std::min(nbytes, size_ - pos);
  peek_buf_.resize(len);
  if (len > 0) {
    ARROW_ASSIGN_OR_RAISE(const int64_t n, ReadAt(pos, len, peek_buf_.data()));
    peek_buf_.resize(n);
  }
  return std::string_view(peek_buf_);
}

arrow::Result<std::shared_ptr<const arrow::KeyValueMetadata>>
RGWRandomAccessFile::ReadMetadata()
{
  ARROW_RETURN_NOT_OK(check_open());
  return metadata_;
}
