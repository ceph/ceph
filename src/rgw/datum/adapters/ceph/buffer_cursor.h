// Contiguous reads from native fragment cursors, with caller-owned scratch storage.
#ifndef CEPH_COMMON_BUFFER_CURSOR_H
 #define CEPH_COMMON_BUFFER_CURSOR_H

#include "common/error_code.h"

#include <algorithm>
#include <cstddef>
#include <span>

namespace ceph::buffer {

// Borrow one fragment when possible; otherwise gather exactly storage.size() bytes.
// The cursor supplies a remaining count and advancing chunks, including empty chunks.
// The result borrows from input for one fragment, or from storage after gathering.
// Scratch storage must not overlap unread input when gathering.
template <typename CursorT>
std::span<const char> read_contiguous(CursorT& input, const std::span<char> storage)
{
  if (storage.size() > input.get_remaining()) {
    throw ::ceph::buffer::end_of_buffer {};
  }
  if (storage.empty()) {
    return storage;
  }

  const char *bytes = nullptr;
  std::size_t first;
  do {
    first = input.get_ptr_and_advance(storage.size(), &bytes);
  } while (0 == first);
  if (first == storage.size()) {
    return {bytes, storage.size()};
  }

  std::copy_n(bytes, first, storage.data());
  for (auto copied = first; copied < storage.size();) {
    const char *fragment = nullptr;
    const auto count = input.get_ptr_and_advance(storage.size() - copied, &fragment);
    std::copy_n(fragment, count, storage.data() + copied);
    copied += count;
  }

  return storage;
}

} // ceph::buffer

#endif // CEPH_COMMON_BUFFER_CURSOR_H
