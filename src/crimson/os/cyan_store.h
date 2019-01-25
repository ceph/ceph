// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
#include "include/uuid.h"

// a just-enough store for reading/writing the superblock
class CyanStore {
  const std::string path;
public:
  CyanStore(const std::string& path)
    : path{path}
  {}

  void write_meta(const std::string& key,
		  const std::string& value);
  std::string read_meta(const std::string& key);
};
