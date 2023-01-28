// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <algorithm>
#include <array>
#include <cstring>
#include <string_view>
#include <tuple>
#include <utility>

#include <boost/optional.hpp>
#include <boost/range/adaptor/reversed.hpp>

namespace rgw {
namespace tar {

static constexpr size_t BLOCK_SIZE = 512;


static inline std::pair<class StatusIndicator,
                        boost::optional<class HeaderView>>
interpret_block(const StatusIndicator& status, ceph::bufferlist& bl);


class StatusIndicator {
  friend std::pair<class StatusIndicator,
                   boost::optional<class HeaderView>>
  interpret_block(const StatusIndicator& status, ceph::bufferlist& bl);

  bool is_empty;
  bool is_eof;

  StatusIndicator()
    : is_empty(false),
      is_eof(false) {
  }

  StatusIndicator(const StatusIndicator& prev_status,
                  const bool is_empty)
  : is_empty(is_empty),
    is_eof(is_empty && prev_status.empty()) {
  }

public:
  bool empty() const {
    return is_empty;
  }

  bool eof() const {
    return is_eof;
  }

  static StatusIndicator create() {
    return StatusIndicator();
  }
} /* class StatusIndicator */;


enum class FileType : char {
  UNKNOWN = '\0',

  /* The tar format uses ASCII encoding. */
  NORMAL_FILE = '0',
  DIRECTORY = '5'
}; /* enum class FileType */

class HeaderView {
protected:
  /* Everything is char here (ASCII encoding), so we don't need to worry about
   * the struct padding. */
  const struct header_t {
    char filename[100];
    char __filemode[8];
    char __owner_id[8];
    char __group_id[8];
    char filesize[12];
    char lastmod[12];
    char checksum[8];
    char filetype;
    char __padding[355];
  } *header;

  static_assert(sizeof(*header) == BLOCK_SIZE,
                "The TAR header must be exactly BLOCK_SIZE length");

  /* The label is far more important from what the code really does. */
  static size_t pos2len(const size_t pos) {
    return pos + 1;
  }

public:
  explicit HeaderView(const char (&header)[BLOCK_SIZE])
    : header(reinterpret_cast<const header_t*>(header)) {
  }

  FileType get_filetype() const {
    switch (header->filetype) {
      case static_cast<char>(FileType::NORMAL_FILE):
        return FileType::NORMAL_FILE;
      case static_cast<char>(FileType::DIRECTORY):
        return FileType::DIRECTORY;
      default:
        return FileType::UNKNOWN;
    }
  }

  std::string_view get_filename() const {
    return std::string_view(header->filename,
                             std::min(sizeof(header->filename),
                                      strlen(header->filename)));
  }

  size_t get_filesize() const {
    /* The string_ref is pretty suitable here because tar encodes its
     * metadata in ASCII. */
    const std::string_view raw(header->filesize, sizeof(header->filesize));

    /* We need to find where the padding ends. */
    const auto pad_ends_at = std::min(raw.find_last_not_of('\0'),
                                      raw.find_last_not_of(' '));
    const auto trimmed = raw.substr(0,
      pad_ends_at == std::string_view::npos ? std::string_view::npos
                                             : pos2len(pad_ends_at));

    size_t sum = 0, mul = 1;
    for (const char c : boost::adaptors::reverse(trimmed)) {
      sum += (c - '0') * mul;
      mul *= 8;
    }

    return sum;
  }
}; /* class Header */


static inline std::pair<StatusIndicator,
                        boost::optional<HeaderView>>
interpret_block(const StatusIndicator& status, ceph::bufferlist& bl) {
  static constexpr std::array<char, BLOCK_SIZE> zero_block = {0, };
  const char (&block)[BLOCK_SIZE] = \
    reinterpret_cast<const char (&)[BLOCK_SIZE]>(*bl.c_str());

  if (std::memcmp(zero_block.data(), block, BLOCK_SIZE) == 0) {
    return std::make_pair(StatusIndicator(status, true), boost::none);
  } else {
    return std::make_pair(StatusIndicator(status, false), HeaderView(block));
  }
}

} /* namespace tar */
} /* namespace rgw */
