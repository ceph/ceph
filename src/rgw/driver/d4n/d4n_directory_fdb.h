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

struct FDBRange {
  std::string begin;
  std::string end;
};

// FDB Transactions are not thread-safe and must not be accessed concurrently.
// A Transaction instance must be used by a single thread at a time.
// Otherwise we have to change the architecture to be thread safe including "executed_" member variable.
class FDBTransaction : public Transaction {
public:
  explicit FDBTransaction(lfdb::database_handle db) : txn_(lfdb::make_transaction(db)) {}

  ~FDBTransaction() override = default;

  lfdb::transaction_handle& get_transaction() { return txn_; }

  int commit(const DoutPrefixProvider* dpp, optional_yield y) override;
  int abort(const DoutPrefixProvider* dpp, optional_yield y) override;

private:
  lfdb::transaction_handle txn_;
  bool executed_{false};
};

class FDBLease : public Lease {
public:
    explicit FDBLease(lfdb::database_handle db)
      : FDBdb(db)
    {
    }

    void set_fdb_database(lfdb::database_handle db)
    {
      FDBdb = db;
    }

    int acquire(const DoutPrefixProvider* dpp,
                const std::string& resource_name,
                const std::string& holder_id,
                const std::string& token,
                uint64_t ttl_nanoseconds) override;

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

class FDBTransactionFactory : public TransactionFactory {
public:
  explicit FDBTransactionFactory(lfdb::database_handle db) : db_(std::move(db)) {}

  std::unique_ptr<Transaction> create_transaction(const DoutPrefixProvider* dpp) override { return std::make_unique<FDBTransaction>(db_); }

private:
  lfdb::database_handle db_;
};


class FDBDirectory : virtual public Directory {
public:
    lfdb::database_handle FDBdb;

    explicit FDBDirectory(lfdb::database_handle db)
      : FDBdb(db)
    {
    }

    virtual ~FDBDirectory() = default;

    void set_fdb_database(lfdb::database_handle db)
    {
        FDBdb = db;
    }

    virtual int get_kv(const DoutPrefixProvider* dpp,
                       optional_yield y,
                       const std::string& key,
                       const std::string& field,
                       std::string& out_val, 
		       std::optional<std::reference_wrapper<Transaction>> txn);

    virtual int set_kv(const DoutPrefixProvider* dpp,
                       optional_yield y,
                       const std::string& key,
                       const std::string& field,
                       const std::string& val,
		       std::optional<std::reference_wrapper<Transaction>> txn);

    virtual int get_kv_multi(const DoutPrefixProvider* dpp,
                             optional_yield y,
                             const std::string& key,
                             const std::vector<std::string>& fields,
                             std::map<std::string, std::string>& out_vals, 
			     std::optional<std::reference_wrapper<Transaction>> txn);

    virtual int set_kv_multi(const DoutPrefixProvider* dpp,
                             optional_yield y,
                             const std::string& key,
                             const std::map<std::string, std::string>& vals,
			     std::optional<std::reference_wrapper<Transaction>> txn);

    virtual int set_kv_if_not_exists(const DoutPrefixProvider* dpp,
                                     optional_yield y,
                                     const std::string& key,
                                     const std::string& field,
                                     const std::string& val,
				     std::optional<std::reference_wrapper<Transaction>> txn);
protected:
  template <typename Func> int with_fdb_transaction(std::optional<std::reference_wrapper<Transaction>> txn, Func&& func);
  template <typename Func> int fdb_invoke(const DoutPrefixProvider* dpp,
		    std::optional<std::reference_wrapper<Transaction>> txn,
		    Func&& operation,
		    std::source_location whence = std::source_location::current());


};

class FDBBucketDirectory : public FDBDirectory, public BucketDirectory {
public:
    explicit FDBBucketDirectory(lfdb::database_handle db)
      : FDBDirectory(db) {}

    virtual int exist_key(const DoutPrefixProvider* dpp, optional_yield y,
                  const std::string& bucket_id,
		  std::optional<std::reference_wrapper<Transaction>> txn) override;

    virtual int del(const DoutPrefixProvider* dpp, optional_yield y,
            const std::string& bucket_id,
	    std::optional<std::reference_wrapper<Transaction>> txn) override;

    virtual int add_object(const DoutPrefixProvider* dpp, optional_yield y,
                   const std::string& bucket_id,
                   const std::string& object_name,
                   std::optional<CacheObject> params,
                   std::optional<std::reference_wrapper<Transaction>> txn) override;

    virtual int remove_object(const DoutPrefixProvider* dpp, optional_yield y,
                      const std::string& bucket_id,
                      const std::string& object_name,
		      std::optional<std::reference_wrapper<Transaction>> txn) override;

    virtual int list_objects(const DoutPrefixProvider* dpp, optional_yield y,
                    const std::string& bucket_id,
                    const std::string& start_token,
                    const std::string& prefix,
                    const std::string& marker,
                    uint64_t count,
                    bool marker_inclusive,
                    std::vector<CacheObject>& objs_info,
                    std::string& continuation_token,
		    std::optional<std::reference_wrapper<Transaction>> txn);

private:
    int collect_range(const DoutPrefixProvider* dpp,
    		      const FDBRange& range,
  		      const std::string& base,
		      uint64_t count,
		      std::vector<CacheObject>& objs_info,
		      std::string& continuation_token,
		      std::optional<std::reference_wrapper<Transaction>> txn);

    FDBRange build_range(const std::string& base,
	  	         const std::string& start,
		         bool inclusive);

    int fdb_add(const DoutPrefixProvider* dpp, optional_yield y,
                const std::string& bucket_id,
                double score,
                const std::string& member,
                std::optional<CacheObject> params,
		std::optional<std::reference_wrapper<Transaction>> txn);

    int fdb_rem(const DoutPrefixProvider* dpp, optional_yield y,
                const std::string& bucket_id,
                const std::string& member,
		std::optional<std::reference_wrapper<Transaction>> txn);

    int fdb_scan(const DoutPrefixProvider* dpp, optional_yield y,
                const std::string& bucket_id,
                const std::string& start_token,
                const std::string& prefix,
                uint64_t count,
                bool marker_inclusive, 
                std::vector<CacheObject>& objs_info,
                std::string& continuation_token,
		std::optional<std::reference_wrapper<Transaction>> txn);

    std::string build_object_index(const std::string& bucket_id, const std::string& obj_name);
};

class FDBObjectDirectory : public FDBDirectory, public ObjectDirectory {
public:
    explicit FDBObjectDirectory(lfdb::database_handle db)
      : FDBDirectory(db) {}

    virtual int exist_key(const DoutPrefixProvider* dpp, optional_yield y,
                  const std::string& bucket_id,
                  const std::string& obj_name,
		  std::optional<std::reference_wrapper<Transaction>> txn) override;

    virtual int del(const DoutPrefixProvider* dpp, optional_yield y,
            CacheObj* object,
	    std::optional<std::reference_wrapper<Transaction>> txn) override;

    virtual int add_version(const DoutPrefixProvider* dpp, optional_yield y,
                    const std::string& bucket_id,
                    const std::string& obj_name,
                    const std::string& version,
                    ceph::real_time& creation_time,
                    std::optional<CacheObjectVersion> params,
                    std::optional<std::reference_wrapper<Transaction>> txn) override;

    virtual int remove_version(const DoutPrefixProvider* dpp, optional_yield y,
                       const std::string& bucket_id,
                       const std::string& obj_name,
                       const std::string& version,
		       std::optional<std::reference_wrapper<Transaction>> txn) override;

    virtual int remove_version_by_creation_time(const DoutPrefixProvider* dpp, optional_yield y,
                                        const std::string& bucket_id,
                                        const std::string& obj_name,
                                        ceph::real_time creation_time,
					std::optional<std::reference_wrapper<Transaction>> txn) override;

    virtual int list_versions(const DoutPrefixProvider* dpp, optional_yield y,
                      const std::string& bucket_id,
                      const std::string& obj_name,
                      const std::string& marker_version,
                      uint64_t count,
                      std::vector<CacheObjectVersion>& obj_versions,
                      std::string& continuation_token,
		      std::optional<std::reference_wrapper<Transaction>> txn) override;

private:
    std::string get_versions_range_end(const std::string& versions_subspace) const;

    bool scan_versions(const DoutPrefixProvider* dpp,
		       optional_yield y,
		       const std::string& begin,
		       const std::string& end,
		       bool reverse,
		       std::vector<std::pair<std::string, 
		       CacheObjectVersion>>& kvs,
		       std::optional<std::reference_wrapper<Transaction>> txn);

    bool parse_version_key(const std::string& versions_subspace,
		           const std::string& key,
		           std::string& score,
		           std::string& member) const;


    int fdb_add(const DoutPrefixProvider* dpp, optional_yield y,
                const std::string& bucket_id,
                const std::string& obj_name,
                int64_t score,
                const std::string& member,
                std::optional<CacheObjectVersion> params,
		std::optional<std::reference_wrapper<Transaction>> txn);

    int fdb_revrange(const DoutPrefixProvider* dpp, optional_yield y,
                    const std::string& bucket_id,
                    const std::string& obj_name,
                    const std::string& marker_version,
                    uint64_t count,
                    std::vector<CacheObjectVersion>& obj_versions,
                    std::string& continuation_token,
		    std::optional<std::reference_wrapper<Transaction>> txn);

    int fdb_rem(const DoutPrefixProvider* dpp, optional_yield y,
                const std::string& bucket_id,
                const std::string& obj_name,
                const std::string& member,
		std::optional<std::reference_wrapper<Transaction>> txn);

    int fdb_remrangebyscore(const DoutPrefixProvider* dpp, optional_yield y,
                            const std::string& bucket_id,
                            const std::string& obj_name,
                            int64_t min,
                            int64_t max,
			    std::optional<std::reference_wrapper<Transaction>> txn);

    int fdb_rank(const DoutPrefixProvider* dpp, optional_yield y,
                 const std::string& bucket_id,
                 const std::string& obj_name,
                 const std::string& member,
                 std::string& index,
		 std::optional<std::reference_wrapper<Transaction>> txn);

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
    explicit FDBBlockDirectory(lfdb::database_handle db)
      : FDBDirectory(db) {}

    int exist_key(const DoutPrefixProvider* dpp, optional_yield y,
                  CacheBlock* block,
		  std::optional<std::reference_wrapper<Transaction>> txn) override;

    int set(const DoutPrefixProvider* dpp, optional_yield y,
            std::vector<CacheBlock>& blocks,
	    std::optional<std::reference_wrapper<Transaction>> txn) override;

    int set(const DoutPrefixProvider* dpp, optional_yield y,
            CacheBlock* block,
            std::optional<std::reference_wrapper<Transaction>> txn) override;

    int get(const DoutPrefixProvider* dpp, optional_yield y,
            CacheBlock* block,
            std::optional<std::reference_wrapper<Transaction>> txn) override;

    int get(const DoutPrefixProvider* dpp, optional_yield y,
            std::vector<CacheBlock>& blocks,
            std::optional<std::reference_wrapper<Transaction>> txn) override;

    int copy(const DoutPrefixProvider* dpp, optional_yield y,
             CacheBlock* block,
             const std::string& copyName,
             const std::string& copyBucketName,
	     std::optional<std::reference_wrapper<Transaction>> txn) override;

    int del(const DoutPrefixProvider* dpp, optional_yield y,
            CacheBlock* block,
	    std::optional<std::reference_wrapper<Transaction>> txn) override;

    int update_field(const DoutPrefixProvider* dpp, optional_yield y,
                     CacheBlock* block,
                     const std::string& field,
                     std::string& value,
	    	     std::optional<std::reference_wrapper<Transaction>> txn) override;

    int remove_host(const DoutPrefixProvider* dpp, optional_yield y,
                    CacheBlock* block,
                    const std::string& value,
	    	    std::optional<std::reference_wrapper<Transaction>> txn) override;

private:
    template <AssociativeContainer Container>
    int set_values(const DoutPrefixProvider* dpp, CacheBlock& block, Container& fdbValues, optional_yield y);

    int populate_block(CacheBlock* block, const std::map<std::string, std::string>& kvs) const;
};

} // namespace rgw::d4n
