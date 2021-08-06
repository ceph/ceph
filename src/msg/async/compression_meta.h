// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "compressor/Compressor.h"

struct CompConnectionMeta {
  TOPNSPC::Compressor::CompressionMode con_mode =
    TOPNSPC::Compressor::COMP_NONE;  // negotiated mode
  TOPNSPC::Compressor::CompressionAlgorithm con_method =
    TOPNSPC::Compressor::COMP_ALG_NONE; // negotiated method

  bool is_compress() const {
    return con_mode != TOPNSPC::Compressor::COMP_NONE;
  }
  TOPNSPC::Compressor::CompressionAlgorithm get_method() const {
    return con_method;
  }
  TOPNSPC::Compressor::CompressionMode get_mode() const {
    return con_mode;
  }
};
