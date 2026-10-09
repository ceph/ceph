// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <functional>
#include <set>
#include <string>
#include <string_view>
#include <vector>

/**
 * Which other modules may read which prefix of a module's KV store,
 * as declared in the owner's SHARED_STORE class attribute. The owner
 * stays the only writer.
 */
class SharedStorePolicy {
public:
  struct Entry {
    std::string prefix;
    std::set<std::string, std::less<>> readers;
  };

  // Returns false and sets *err if the rule is invalid.
  bool add(const std::string &prefix,
           const std::set<std::string> &readers,
           std::string *err) {
    if (prefix.empty()) {
      *err = "prefix must not be empty";
      return false;
    }
    if (prefix.front() == '/') {
      *err = "prefix must not start with '/'";
      return false;
    }
    if (prefix.back() != '/') {
      *err = "prefix must end with '/'";
      return false;
    }
    if (readers.empty()) {
      *err = "readers must not be empty";
      return false;
    }
    Entry e;
    e.prefix = prefix;
    for (const auto &r : readers) {
      if (r.empty()) {
        *err = "reader name must not be empty";
        return false;
      }
      if (r == "*") {
        *err = "wildcard readers are not allowed, name the modules";
        return false;
      }
      if (r.find('/') != std::string::npos) {
        *err = "reader '" + r + "' is not a module name";
        return false;
      }
      e.readers.insert(r);
    }
    entries.push_back(std::move(e));
    return true;
  }

  /// May the module `reader` read `key` (relative to the owner's namespace)?
  bool allows(std::string_view reader, std::string_view key) const {
    for (const auto &e : entries) {
      if (key.substr(0, e.prefix.size()) == e.prefix &&
          e.readers.find(reader) != e.readers.end()) {
        return true;
      }
    }
    return false;
  }

  bool empty() const {
    return entries.empty();
  }

  size_t size() const {
    return entries.size();
  }

private:
  std::vector<Entry> entries;
};
