// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <iterator>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/split.h"
#include "include/compat.h"
#include "rgw_b64.h"
#include "rgw_hex.h"

namespace rgw::sts {

// the token-sealing keyring lives in the mon config-key store
inline constexpr std::string_view STS_KEYRING_CONFIG_KEY = "rgw/sts/keys";
inline constexpr std::string_view STS_LEGACY_KEY_CONFIG_KEY = "rgw/sts/legacy_key";

inline constexpr std::size_t STS_AEAD_KEY_ID_SIZE = 20;
inline constexpr std::size_t STS_AEAD_KEY_SIZE = 32;
inline constexpr std::size_t STS_AEAD_MAX_KEYS = 16;

// 40-char hex form of a raw key id
inline std::string hex_id(const std::string& raw_id)
{
  std::string hex;
  buf_to_hex(raw_id, std::back_inserter(hex));
  return hex;
}

/*
 * decode a 40-char hex id into its raw bytes; hex_to_buf requires
 * NUL-terminated input
 */
inline int parse_hex_id(const std::string& hex, std::string& raw_id)
{
  raw_id.resize(STS_AEAD_KEY_ID_SIZE);
  if (hex_to_buf(hex.c_str(), raw_id.data(),
                 static_cast<int>(raw_id.size())) !=
      static_cast<int>(raw_id.size())) {
    return -EINVAL;
  }
  return 0;
}

// strip trailing whitespace from a stored legacy key value
inline void trim_legacy_key(std::string& key)
{
  if (const auto end = key.find_last_not_of(" \t\r\n"); end != key.npos) {
    key.resize(end + 1);
  } else {
    key.clear();
  }
}

/*
 * a sealing key: a 20-byte id and a 32-byte key. the key is wiped when it is
 * overwritten or destroyed
 */
struct sts_aead_key {
  std::string id;
  std::string key;

  sts_aead_key() = default;
  sts_aead_key(sts_aead_key&&) = default;
  sts_aead_key& operator=(sts_aead_key&& other) noexcept
  {
    if (this != &other) {
      ceph_memzero_s(key.data(), key.size(), key.size());
      id = std::move(other.id);
      key = std::move(other.key);
    }
    return *this;
  }
  ~sts_aead_key()
  {
    ceph_memzero_s(key.data(), key.size(), key.size());
  }
};

/*
 * An ordered set of sealing keys. parse() and the mutation helpers keep it
 * valid: at most 16 entries, 40-hex ids, keys that decode to 32 bytes from
 * canonical base64, and no repeated ids or key material. The first entry
 * seals new tokens; the rest are kept to verify older ones.
 */
class StsKeyring {
  std::vector<sts_aead_key> keys;

  static int decode_entry(std::string_view entry, sts_aead_key& key,
                          std::string& err)
  {
    const auto separator = entry.find('=');
    if (separator == entry.npos) {
      err = "sts keyring has an invalid entry";
      return -EINVAL;
    }
    const auto id_hex = entry.substr(0, separator);
    const auto encoded_key = entry.substr(separator + 1);

    if (parse_hex_id(std::string{id_hex}, key.id) < 0) {
      err = "sts keyring has a key id that is not 40 hexadecimal characters";
      return -EINVAL;
    }

    try {
      key.key = rgw::from_base64(encoded_key);
    } catch (...) {
      err = "sts keyring has a key that is not valid base64";
      return -EINVAL;
    }
    if (key.key.size() != STS_AEAD_KEY_SIZE) {
      err = "sts keyring has a key that does not decode to 32 bytes";
      return -EINVAL;
    }

    std::string reencoded = rgw::to_base64(key.key);
    const bool canonical = reencoded == encoded_key;
    ceph_memzero_s(reencoded.data(), reencoded.size(), reencoded.size());
    if (! canonical) {
      err = "sts keyring has a key that is not canonical base64";
      return -EINVAL;
    }
    return 0;
  }

  int check_unique(const sts_aead_key& key, std::string& err) const
  {
    for (const auto& existing : keys) {
      if (existing.id == key.id) {
        err = "sts keyring has a duplicate key id";
        return -EINVAL;
      }
      if (existing.key == key.key) {
        err = "sts keyring has duplicate key material";
        return -EINVAL;
      }
    }
    return 0;
  }

public:
  // parse whitespace-separated <hex-id>=<base64-key> entries
  static int parse(std::string_view text, StsKeyring& out, std::string& err)
  {
    err.clear();
    if (text.empty()) {
      err = "sts keyring is empty";
      return -EINVAL;
    }

    StsKeyring result;
    result.keys.reserve(STS_AEAD_MAX_KEYS);
    // ceph::split's default delimiter set includes '='; split on whitespace
    for (std::string_view entry : ceph::split(text, " \t\r\n")) {
      if (result.keys.size() == STS_AEAD_MAX_KEYS) {
        err = "sts keyring accepts at most 16 keys";
        return -EINVAL;
      }
      sts_aead_key key;
      if (int r = decode_entry(entry, key, err); r < 0) {
        return r;
      }
      if (int r = result.check_unique(key, err); r < 0) {
        return r;
      }
      result.keys.push_back(std::move(key));
    }
    if (result.keys.empty()) {
      err = "sts keyring contains no keys";
      return -EINVAL;
    }
    out = std::move(result);
    return 0;
  }

  // one <hex-id>=<base64-key> entry per line, in verification order
  std::string format() const
  {
    std::string out;
    for (const auto& key : keys) {
      out += hex_id(key.id);
      out.push_back('=');
      out += rgw::to_base64(key.key);
      out.push_back('\n');
    }
    return out;
  }

  std::size_t size() const { return keys.size(); }
  const std::vector<sts_aead_key>& entries() const { return keys; }

  // seals new tokens. only valid when the keyring isn't empty
  const sts_aead_key& sealing_key() const { return keys.front(); }

  const sts_aead_key* find(std::string_view id) const
  {
    for (const auto& key : keys) {
      if (key.id == id) {
        return &key;
      }
    }
    return nullptr;
  }

  // prepend a new sealing key; rejects a duplicate or a full keyring
  int prepend(sts_aead_key key, std::string& err)
  {
    if (keys.size() >= STS_AEAD_MAX_KEYS) {
      err = "the sts keyring cannot hold more than 16 keys; retire one with"
            " 'sts keyring trim' or 'sts keyring rm' first";
      return -EINVAL;
    }
    if (int r = check_unique(key, err); r < 0) {
      return r;
    }
    keys.insert(keys.begin(), std::move(key));
    return 0;
  }

  // remove the key with this raw id; rejects an unknown id or the only key
  int remove(std::string_view id, std::string& err)
  {
    auto it = std::find_if(keys.begin(), keys.end(),
                           [&](const sts_aead_key& k) { return k.id == id; });
    if (it == keys.end()) {
      err = "the sts keyring has no such key";
      return -ENOENT;
    }
    if (keys.size() == 1) {
      err = "refusing to remove the sts keyring's only key";
      return -EINVAL;
    }
    keys.erase(it);
    return 0;
  }

  /*
   * drop the oldest entries beyond keep (keep may be 0); returns the raw ids
   * removed
   */
  std::vector<std::string> trim(std::size_t keep)
  {
    std::vector<std::string> removed;
    while (keys.size() > keep) {
      removed.push_back(keys.back().id);
      keys.pop_back();
    }
    return removed;
  }

  // move the keys out, leaving the keyring empty
  std::vector<sts_aead_key> release() { return std::move(keys); }
};

} // namespace rgw::sts
