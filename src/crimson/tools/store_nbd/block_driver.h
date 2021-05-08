// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>

#include <seastar/core/future.hh>

#include <string>
#include <optional>

#include "include/buffer.h"

/**
 * BlockDriver
 *
 * Simple interface to enable throughput test to compare raw disk to
 * transaction_manager, etc
 */
class BlockDriver {
public:
  struct config_t {
    std::string type;
    bool mkfs = false;
    unsigned num_collections = 128;
    unsigned object_size = 4<<20 /* 4MB, rbd default */;
    std::optional<std::string> path;

    bool is_futurized_store() const {
      return type == "seastore" || type == "bluestore";
    }

    std::string get_fs_type() const {
      ceph_assert(is_futurized_store());
      return type;
    }

    void populate_options(
      boost::program_options::options_description &desc)
    {
      namespace po = boost::program_options;
      desc.add_options()
	("type",
	 po::value<std::string>()
	 ->default_value("transaction_manager")
	 ->notifier([this](auto s) { type = s; }),
	 "Backend to use, options are transaction_manager, seastore"
	)
	("device-path",
	 po::value<std::string>()
	 ->required()
	 ->notifier([this](auto s) { path = s; }),
	 "Path to device for backend"
	)
	("num-collections",
	 po::value<unsigned>()
	 ->notifier([this](auto s) { num_collections = s; }),
	 "Number of collections to use for futurized_store backends"
	)
	("object-size",
	 po::value<unsigned>()
	 ->notifier([this](auto s) { object_size = s; }),
	 "Object size to use for futurized_store backends"
	)
	("mkfs",
	 po::value<bool>()
	 ->default_value(false)
	 ->notifier([this](auto s) { mkfs = s; }),
	 "Do mkfs first"
	);
    }
  };

  virtual ceph::bufferptr get_buffer(size_t size) = 0;

  virtual seastar::future<> write(
    off_t offset,
    ceph::bufferptr ptr) = 0;

  virtual seastar::future<ceph::bufferlist> read(
    off_t offset,
    size_t size) = 0;

  virtual size_t get_size() const = 0;

  virtual seastar::future<> mount() = 0;
  virtual seastar::future<> close() = 0;

  virtual ~BlockDriver() {}
};
using BlockDriverRef = std::unique_ptr<BlockDriver>;

BlockDriverRef get_backend(BlockDriver::config_t config);
