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

namespace rgw::d4n {

class FDBDirectory : virtual public Directory {
public:
    // FoundationDB database handle
    lfdb::database_handle FDBconn;

    explicit FDBDirectory(const std::shared_ptr<FDBConnection>& fdb_conn)
      : FDBconn(fdb_conn->get_fdb_conn())
    {
    }

    virtual ~FDBDirectory() = default;

    void set_fdb_database(lfdb::database_handle db)
    {
        FDBconn = std::move(db);
    }

    virtual int get_kv(const DoutPrefixProvider* dpp,
                       optional_yield y,
                       const std::string& key,
                       const std::string& field,
                       std::string& out_val);

    virtual int set_kv(const DoutPrefixProvider* dpp,
                       optional_yield y,
                       const std::string& key,
                       const std::string& field,
                       const std::string& val);

    virtual int get_kv_multi(const DoutPrefixProvider* dpp,
                             optional_yield y,
                             const std::string& key,
                             const std::vector<std::string>& fields,
                             std::map<std::string, std::string>& out_vals);

    virtual int set_kv_multi(const DoutPrefixProvider* dpp,
                             optional_yield y,
                             const std::string& key,
                             const std::map<std::string, std::string>& vals);

    virtual int set_kv_if_not_exists(const DoutPrefixProvider* dpp,
                                     optional_yield y,
                                     const std::string& key,
                                     const std::string& field,
                                     const std::string& val);
};

class FDBBucketDirectory : public FDBDirectory, public BucketDirectory {
public:
    explicit FDBBucketDirectory(const std::shared_ptr<FDBConnection>& fdb_conn)
      : FDBDirectory(fdb_conn) {}

    int exist_key(const DoutPrefixProvider* dpp,
                  const std::string& bucket_id,
                  optional_yield y) override;

    int del(const DoutPrefixProvider* dpp,
            const std::string& bucket_id,
            optional_yield y) override;

    int add_object(const DoutPrefixProvider* dpp,
                   const std::string& bucket_id,
                   const std::string& object_name,
                   std::optional<CacheObject> params,
                   optional_yield y,
                   Pipeline* pipeline = nullptr) override;

    int remove_object(const DoutPrefixProvider* dpp,
                      const std::string& bucket_id,
                      const std::string& object_name,
                      optional_yield y) override;

    int list_objects(const DoutPrefixProvider* dpp,
                    const std::string& bucket_id,
                    const std::string& start_token,
                    const std::string& prefix,
                    const std::string& marker,
                    uint64_t count,
                    bool marker_inclusive,
                    std::vector<CacheObject>& objs_info,
                    std::string& continuation_token,
                    optional_yield y);

private:
    int fdb_add(const DoutPrefixProvider* dpp,
                const std::string& bucket_id,
                double score,
                const std::string& member,
                std::optional<CacheObject> params,
                optional_yield y);

    int fdb_rem(const DoutPrefixProvider* dpp,
                const std::string& bucket_id,
                const std::string& member,
                optional_yield y);

    int fdb_range(const DoutPrefixProvider* dpp,
                  const std::string& bucket_id,
                  const std::string& start,
                  uint64_t count,
                  std::vector<CacheObject>& objs_info,
                  std::string& continuation_token,
                  bool start_inclusive,
                  optional_yield y);

    int fdb_scan(const DoutPrefixProvider* dpp,
                const std::string& bucket_id,
                const std::string& start_token,
                const std::string& prefix,
                uint64_t count,
                bool marker_inclusive, 
                std::vector<CacheObject>& objs_info,
                std::string& continuation_token,
                optional_yield y);

    std::string build_object_index(const std::string& bucket_id,const std::string& obj_name);
    std::string get_object_subspace(const std::string& bucket_id);
};

class FDBObjectDirectory : public FDBDirectory, public ObjectDirectory {
public:
    explicit FDBObjectDirectory(const std::shared_ptr<FDBConnection>& fdb_conn)
      : FDBDirectory(fdb_conn) {}

    int exist_key(const DoutPrefixProvider* dpp,
                  const std::string& bucket_id,
                  const std::string& obj_name,
                  optional_yield y) override;

    int del(const DoutPrefixProvider* dpp,
            CacheObj* object,
            optional_yield y) override;

    int add_version(const DoutPrefixProvider* dpp,
                    const std::string& bucket_id,
                    const std::string& obj_name,
                    const std::string& version,
                    ceph::real_time& creation_time,
                    std::optional<CacheObjectVersion> params,
                    optional_yield y,
                    Pipeline* pipeline = nullptr) override;

    int remove_version(const DoutPrefixProvider* dpp,
                       const std::string& bucket_id,
                       const std::string& obj_name,
                       const std::string& version,
                       optional_yield y) override;

    int remove_version_by_creation_time(const DoutPrefixProvider* dpp,
                                        const std::string& bucket_id,
                                        const std::string& obj_name,
                                        ceph::real_time creation_time,
                                        optional_yield y) override;

    int list_versions(const DoutPrefixProvider* dpp,
                      const std::string& bucket_id,
                      const std::string& obj_name,
                      const std::string& marker_version,
                      uint64_t count,
                      std::vector<CacheObjectVersion>& obj_versions,
                      std::string& continuation_token,
                      optional_yield y);

private:
    int fdb_add(const DoutPrefixProvider* dpp,
                const std::string& bucket_id,
                const std::string& obj_name,
                int64_t score,
                const std::string& member,
                std::optional<CacheObjectVersion> params,
                optional_yield y);

    int fdb_range(const DoutPrefixProvider* dpp,
                  const std::string& bucket_id,
                  const std::string& obj_name,
                  int start,
                  int stop,
                  std::vector<std::string>& members,
                  optional_yield y);

    int fdb_revrange(const DoutPrefixProvider* dpp,
                    const std::string& bucket_id,
                    const std::string& obj_name,
                    const std::string& marker_version,
                    uint64_t count,
                    std::vector<CacheObjectVersion>& obj_versions,
                    std::string& continuation_token,
                    optional_yield y);

    int fdb_rem(const DoutPrefixProvider* dpp,
                const std::string& bucket_id,
                const std::string& obj_name,
                const std::string& member,
                optional_yield y);

    int fdb_remrangebyscore(const DoutPrefixProvider* dpp,
                            const std::string& bucket_id,
                            const std::string& obj_name,
                            int64_t min,
                            int64_t max,
                            optional_yield y);

    int fdb_rank(const DoutPrefixProvider* dpp,
                 const std::string& bucket_id,
                 const std::string& obj_name,
                 const std::string& member,
                 std::string& index,
                 optional_yield y);

    std::string get_versions_subspace(const DoutPrefixProvider* dpp,
                                        const std::string& bucket_id,
                                        const std::string& obj_name);
    std::string get_score_subspace(const DoutPrefixProvider* dpp,
                                        const std::string& bucket_id,
                                        const std::string& obj_name);
    std::string build_versions_index(const DoutPrefixProvider* dpp,
                                        const std::string& bucket_id,
                                        const std::string& obj_name,
                                        const std::string& score,
                                        const std::string& version);
    std::string build_version_score_index(const DoutPrefixProvider* dpp,
                                                const std::string& bucket_id,
                                                const std::string& obj_name,
                                                const std::string& version);
};

class FDBBlockDirectory : public FDBDirectory, public BlockDirectory {
public:
    explicit FDBBlockDirectory(const std::shared_ptr<FDBConnection>& fdb_conn)
      : FDBDirectory(fdb_conn) {}

    int exist_key(const DoutPrefixProvider* dpp,
                  CacheBlock* block,
                  optional_yield y) override;

    int set(const DoutPrefixProvider* dpp,
            std::vector<CacheBlock>& blocks,
            optional_yield y) override;

    int set(const DoutPrefixProvider* dpp,
            CacheBlock* block,
            optional_yield y,
            Pipeline* pipeline = nullptr) override;

    int get(const DoutPrefixProvider* dpp,
            CacheBlock* block,
            optional_yield y) override;

    int get(const DoutPrefixProvider* dpp,
            std::vector<CacheBlock>& blocks,
            optional_yield y) override;

    int copy(const DoutPrefixProvider* dpp,
             CacheBlock* block,
             const std::string& copyName,
             const std::string& copyBucketName,
             optional_yield y) override;

    int del(const DoutPrefixProvider* dpp,
            CacheBlock* block,
            optional_yield y) override;

    int update_field(const DoutPrefixProvider* dpp,
                     CacheBlock* block,
                     const std::string& field,
                     std::string& value,
                     optional_yield y) override;

    int remove_host(const DoutPrefixProvider* dpp,
                    CacheBlock* block,
                    std::string& value,
                    optional_yield y) override;

private:
    template <AssociativeContainer Container>
    int set_values(const DoutPrefixProvider* dpp,
                   CacheBlock& block,
                   Container& fdbValues,
                   optional_yield y);
};

} // namespace rgw::d4n
