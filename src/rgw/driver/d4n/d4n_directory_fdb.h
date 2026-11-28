/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 International Business Machines Corp. (IBM)
 *      
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
*/

#pragma once

#include "d4n_directory.h"
#include "rgw/ceph_fdb.h"

#include <fmt/format.h>
#include <fmt/chrono.h>
#include <fmt/ranges.h>

#include "include/random.h"

#include <chrono>
#include <optional>
#include <utility>
#include <vector>

using fmt::format;
using fmt::println;
using std::end;
using std::begin;
using std::string;
using std::string_view;
using std::to_string;
using std::vector;

using namespace std::literals::string_literals;

namespace lfdb = ceph::libfdb;
namespace fdbc = lfdb::layer::content;

namespace rgw::d4n {

class FDBLease : public Lease {
public:
    explicit FDBLease(lfdb::database_handle db)
      : FDBdb(db)
    {
    }

    int acquire(const DoutPrefixProvider* dpp,
    		const std::string& resource_name,
    		const std::string& holder_id,
    		const std::string& token,
    		uint64_t ttl_nanoseconds,
                std::optional<Transaction> txn = std::nullopt) override;

    int renew(const DoutPrefixProvider* dpp,
              const std::string& resource_name,
              const std::string& holder_id,
              const std::string& token,
              uint64_t ttl_nanoseconds,
              uint64_t max_ticks = 0) override;

    int release(const DoutPrefixProvider* dpp,
                const std::string& resource_name,
                const std::string& holder_id,
                const std::string& token) override;

    LeaseCheckResult any_active(const DoutPrefixProvider* dpp,
                                 const std::string& resource_prefix) override;

    LeaseCheckResult is_active(const DoutPrefixProvider* dpp,
                                const std::string& resource_name,
                                const std::string& holder_id,
                                const std::string& token) override;

private:
    lfdb::database_handle FDBdb;
};

class FDBDirectory : virtual public Directory {
public:
    lfdb::database_handle FDBdb;

    explicit FDBDirectory(lfdb::database_handle db)
      : FDBdb(db)
    {
    }

    virtual ~FDBDirectory() = default;

    virtual int get_kv(const DoutPrefixProvider* dpp,
                       optional_yield y,
                       const std::string& key,
                       const std::string& field,
                       std::string& out_val, 
                       std::optional<Transaction> txn);

    virtual int set_kv(const DoutPrefixProvider* dpp,
                       optional_yield y,
                       const std::string& key,
                       const std::string& field,
                       const std::string& val,
                       std::optional<Transaction> txn);

    virtual int get_kv_multi(const DoutPrefixProvider* dpp,
                             optional_yield y,
                             const std::string& key,
                             const std::vector<std::string>& fields,
                             std::map<std::string, std::string>& out_vals, 
                             std::optional<Transaction> txn);

    virtual int set_kv_multi(const DoutPrefixProvider* dpp,
                             optional_yield y,
                             const std::string& key,
                             const std::map<std::string, std::string>& vals,
                             std::optional<Transaction> txn);

    virtual int set_kv_if_not_exists(const DoutPrefixProvider* dpp,
                                     optional_yield y,
                                     const std::string& key,
                                     const std::string& field,
                                     const std::string& val,
                                     std::optional<Transaction> txn);

};

class FDBBucketDirectory : public FDBDirectory, public BucketDirectory {
    static constexpr std::string_view object_listing_segment = "objects";

public:
    explicit FDBBucketDirectory(lfdb::database_handle db)
      : FDBDirectory(db) {}

    virtual int exist_key(const DoutPrefixProvider* dpp, optional_yield y,
                  const std::string& bucket_id,
                  std::optional<Transaction> txn) override;

    virtual int del(const DoutPrefixProvider* dpp, optional_yield y,
            const std::string& bucket_id,
            std::optional<Transaction> txn) override;

    virtual int add_object(const DoutPrefixProvider* dpp, optional_yield y,
                   const std::string& bucket_id,
                   const std::string& object_name,
                   std::optional<CacheObject> params,
                   std::optional<Transaction> txn) override;

    virtual int remove_object(const DoutPrefixProvider* dpp, optional_yield y,
                      const std::string& bucket_id,
                      const std::string& object_name,
                      std::optional<Transaction> txn) override;

    virtual int list_objects(const DoutPrefixProvider* dpp, optional_yield y,
                    const std::string& bucket_id,
                    const std::string& start_token,
                    const std::string& prefix,
                    const std::string& marker,
                    std::string_view end_marker,
                    uint64_t count,
                    bool marker_inclusive,
                    std::vector<CacheObject>& objs_info,
                    std::string& continuation_token,
                    std::optional<Transaction> txn) override;

private:
    static fdbc::compiled_key object_listing_subspace(std::string_view bucket_id);
    static fdbc::compiled_key build_object_index(std::string_view bucket_id, std::string_view obj_name);
    static fdbc::compiled_key build_object_index(fdbc::compiled_key parent, std::string_view obj_name);
    static lfdb::query::interval object_listing_query(
        std::string_view bucket_id, std::string_view prefix, std::string_view marker,
        std::string_view end_marker, bool marker_inclusive);
};

class FDBObjectDirectory : public FDBDirectory, public ObjectDirectory {
public:
    explicit FDBObjectDirectory(lfdb::database_handle db)
      : FDBDirectory(db) {}

    virtual int exist_key(const DoutPrefixProvider* dpp, optional_yield y,
                  const std::string& bucket_id,
                  const std::string& obj_name,
                  std::optional<Transaction> txn) override;

    virtual int del(const DoutPrefixProvider* dpp, optional_yield y,
            CacheObj* object,
            std::optional<Transaction> txn) override;

    virtual int add_version(const DoutPrefixProvider* dpp, optional_yield y,
                    const std::string& bucket_id,
                    const std::string& obj_name,
                    const std::string& version,
                    ceph::real_time& creation_time,
                    std::optional<CacheObjectVersion> params,
                    std::optional<Transaction> txn) override;

    virtual int remove_version(const DoutPrefixProvider* dpp, optional_yield y,
                       const std::string& bucket_id,
                       const std::string& obj_name,
                       const std::string& version,
                       std::optional<Transaction> txn) override;

    virtual int remove_version_by_creation_time(const DoutPrefixProvider* dpp, optional_yield y,
                                        const std::string& bucket_id,
                                        const std::string& obj_name,
                                        ceph::real_time creation_time,
                                        std::optional<Transaction> txn) override;

    virtual int list_versions(const DoutPrefixProvider* dpp, optional_yield y,
                      const std::string& bucket_id,
                      const std::string& obj_name,
                      const std::string& marker_version,
                      uint64_t count,
                      std::vector<CacheObjectVersion>& obj_versions,
                      std::string& continuation_token,
                      std::optional<Transaction> txn) override;

private:
    static fdbc::compiled_key object_keyspace(const std::string& bucket_id, const std::string& obj_name);
    static std::pair<fdbc::compiled_key, fdbc::compiled_key>
    version_subspaces(const std::string& bucket_id, const std::string& obj_name);
    static fdbc::compiled_key get_versions_subspace(fdbc::compiled_key object_index);
    static fdbc::compiled_key get_score_subspace(fdbc::compiled_key object_index);
    static fdbc::compiled_key build_versions_score_prefix(
        fdbc::compiled_key versions_subspace, std::string_view score);
    static fdbc::compiled_key build_versions_index(
        fdbc::compiled_key versions_subspace, std::string_view score, std::string_view version);
    static fdbc::compiled_key build_version_score_index(
        fdbc::compiled_key score_subspace, std::string_view version);
    static lfdb::query::interval version_score_query(
        const fdbc::compiled_key& versions_subspace, int64_t min, int64_t max);

};

class FDBBlockDirectory : public FDBDirectory, public BlockDirectory {
public:
    explicit FDBBlockDirectory(lfdb::database_handle db)
      : FDBDirectory(db) {}

    int exist_key(const DoutPrefixProvider* dpp, optional_yield y,
                  CacheBlock* block,
                  std::optional<Transaction> txn) override;

    int set(const DoutPrefixProvider* dpp, optional_yield y,
            std::vector<CacheBlock>& blocks,
            std::optional<Transaction> txn) override;

    int set(const DoutPrefixProvider* dpp, optional_yield y,
            CacheBlock* block,
            std::optional<Transaction> txn) override;

    using BlockDirectory::get;

    std::expected<std::optional<CacheBlock>, int>
    find(const DoutPrefixProvider *dpp, optional_yield y, const CacheBlock& key,
         std::optional<Transaction> txn = std::nullopt) override;

    int get(const DoutPrefixProvider* dpp, optional_yield y,
            std::vector<CacheBlock>& blocks,
            std::optional<Transaction> txn) override;

    int copy(const DoutPrefixProvider* dpp, optional_yield y,
             CacheBlock* block,
             const std::string& copyName,
             const std::string& copyBucketName,
             std::optional<Transaction> txn) override;

    int del(const DoutPrefixProvider* dpp, optional_yield y,
            CacheBlock* block,
            std::optional<Transaction> txn) override;

    int update_field(const DoutPrefixProvider* dpp, optional_yield y,
                     CacheBlock* block,
                     const std::string& field,
                     std::string& value,
                     std::optional<Transaction> txn) override;

    int remove_host(const DoutPrefixProvider* dpp, optional_yield y,
                    CacheBlock* block,
                    const std::string& value,
                    std::optional<Transaction> txn) override;

private:
    std::optional<CacheBlock> read_block(const lfdb::transaction_handle& txn,
                                       const std::string& key) const;

    int populate_block(CacheBlock& block, const std::map<std::string, std::string>& kvs) const;
};

} // namespace rgw::d4n
