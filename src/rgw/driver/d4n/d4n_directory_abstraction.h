#pragma once

namespace rgw { namespace d4n {

  struct CacheObj
  {

  };

  struct CacheBlock
  {

  };

  /**
   * Abstract interface for backend database operations
   * Hides implementation details of Redis, FoundationDB, etc.
   */
  class BackendConnection {
  public:
    virtual ~BackendConnection() = default;
    virtual int initialize(const DoutPrefixProvider *dpp) = 0;
    virtual void shutdown() = 0;
  };

  /**
   * Range query parameters - bucket listing
   */
  struct RangeQuery {
    std::string start_key;
    std::string end_key;
    uint64_t offset = 0;
    uint64_t limit = 0;
    bool reverse = false;
    std::string pattern;

    // For cursor-based iteration
    std::optional<uint64_t> cursor;
    std::optional<uint64_t> next_cursor;
  };

  /**
   * Ordered set operations abstraction
   */
  class OrderedSetOps {
  public:
    virtual ~OrderedSetOps() = default;

    // Add member with score/ordering
    virtual int add_member(const DoutPrefixProvider* dpp,
                          const std::string& key,
                          double score,
                          const std::string& member,
                          optional_yield y) = 0;

    // Remove member
    virtual int remove_member(const DoutPrefixProvider* dpp,
                            const std::string& key,
                            const std::string& member,
                            optional_yield y) = 0;

    // Remove members by score range
    virtual int remove_by_score_range(const DoutPrefixProvider* dpp,
                                    const std::string& key,
                                    double min_score,
                                    double max_score,
                                    optional_yield y) = 0;

    // Get rank/position of member
    virtual int get_rank(const DoutPrefixProvider* dpp,
                        const std::string& key,
                        const std::string& member,
                        uint64_t& rank,
                        optional_yield y) = 0;

    // Range query with various options
    virtual int query_range(const DoutPrefixProvider* dpp,
                          const std::string& key,
                          const RangeQuery& query,
                          std::vector<std::string>& results,
                          optional_yield y) = 0;
  };

  /**
   * Key-value operations abstraction
   */
  class KeyValueOps {
  public:
    virtual ~KeyValueOps() = default;

    // Check if key exists
    virtual int exists(const DoutPrefixProvider* dpp,
                      const std::string& key,
                      optional_yield y) = 0;

    // Set key-value with fields
    virtual int set(const DoutPrefixProvider* dpp,
                  const std::string& key,
                  const std::map<std::string, std::string>& fields,
                  optional_yield y) = 0;

    // Get all fields for a key
    virtual int get(const DoutPrefixProvider* dpp,
                  const std::string& key,
                  std::map<std::string, std::string>& fields,
                  optional_yield y) = 0;

    // Update specific field
    virtual int update_field(const DoutPrefixProvider* dpp,
                            const std::string& key,
                            const std::string& field,
                            const std::string& value,
                            optional_yield y) = 0;

    // Delete key
    virtual int del(const DoutPrefixProvider* dpp,
                   const std::string& key,
                   optional_yield y) = 0;

    // Copy key
    virtual int copy(const DoutPrefixProvider* dpp,
                    const std::string& src_key,
                    const std::string& dst_key,
                    optional_yield y) = 0;
  };

  /**
   * Batch/Pipelined operations
   */
  class BatchOps {
  public:
      virtual ~BatchOps() = default;

      virtual void begin_batch() = 0;
      virtual int execute_batch(const DoutPrefixProvider* dpp, optional_yield y) = 0;
      virtual void clear_batch() = 0;
      virtual bool is_batching() const = 0;
  };

  class Backend : public BackendConnection,
                 public OrderedSetOps,
                 public KeyValueOps,
                 public BatchOps {
  public:
      virtual ~Backend() = default;
  };

  /**
   * Base directory class with common functionality
   */
  class Directory {
  public:
      Directory(std::shared_ptr<Backend> backend) : backend(backend) {}
      virtual ~Directory() = default;

  protected:
      std::shared_ptr<Backend> backend;
  };

  /**
   * Bucket Directory - manages bucket-level metadata and ordered object lists
   */
  class BucketDirectory : public Directory {
  public:
    BucketDirectory(std::shared_ptr<IBackend> backend) : Directory(backend) {}

    // Add object to bucket's ordered index
    int add_object(const DoutPrefixProvider* dpp,
                  const std::string& bucket_id,
                  const std::string& object_name,
                  double timestamp,
                  optional_yield y);

    // Remove object from bucket's ordered index
    int remove_object(const DoutPrefixProvider* dpp,
                      const std::string& bucket_id,
                      const std::string& object_name,
                      optional_yield y);

    // List objects in bucket with pagination
    int list_objects(const DoutPrefixProvider* dpp,
                    const std::string& bucket_id,
                    const RangeQuery& query,
                    std::vector<std::string>& objects,
                    optional_yield y);

    // Get object position in bucket
    int get_object_rank(const DoutPrefixProvider* dpp,
                      const std::string& bucket_id,
                      const std::string& object_name,
                      uint64_t& rank,
                      optional_yield y);

  private:
    std::string build_index(const std::string& bucket_id) const;
  };

  /**
   * Object Directory - manages object metadata
   */
  class ObjectDirectory : public Directory {
  public:
    ObjectDirectory(std::shared_ptr<IBackend> backend) : Directory(backend) {}

    // Check if object exists
    int exists(const DoutPrefixProvider* dpp,
              const CacheObj* object,
              optional_yield y);

    // Store object metadata
    int set(const DoutPrefixProvider* dpp,
            const CacheObj* object,
            optional_yield y);

    // Retrieve object metadata
    int get(const DoutPrefixProvider* dpp,
            CacheObj* object,
            optional_yield y);

    // Copy object metadata
    int copy(const DoutPrefixProvider* dpp,
            const CacheObj* object,
            const std::string& dest_name,
            const std::string& dest_bucket,
            optional_yield y);

    // Delete object metadata
    int del(const DoutPrefixProvider* dpp,
            const CacheObj* object,
            optional_yield y);

    // Update specific field
    int update_field(const DoutPrefixProvider* dpp,
                    const CacheObj* object,
                    const std::string& field,
                    const std::string& value,
                    optional_yield y);

    // Add object versions (ordered by timestamp)
    int add_version(const DoutPrefixProvider* dpp,
                    const CacheObj* object,
                    const std::string& version_id,
                    double timestamp,
                    optional_yield y);

    int list_versions(const DoutPrefixProvider* dpp,
                      const CacheObj* object,
                      const RangeQuery& query,
                      std::vector<std::string>& versions,
                      optional_yield y);

    int remove_version(const DoutPrefixProvider* dpp,
                      const CacheObj* object,
                      const std::string& version_id,
                      optional_yield y);

  private:
    std::string build_index(const CacheObj* object) const;
  };

  /**
   * Block Directory - manages block-level metadata
   */
  class BlockDirectory : public Directory {
  public:
    BlockDirectory(std::shared_ptr<IBackend> backend) : Directory(backend) {}

    // Check if block exists
    int exists(const DoutPrefixProvider* dpp,
              const CacheBlock* block,
              optional_yield y);

    // Store block metadata (single)
    int set(const DoutPrefixProvider* dpp,
            const CacheBlock* block,
            optional_yield y);

    // Store multiple blocks (batched)
    int set_batch(const DoutPrefixProvider* dpp,
                  const std::vector<CacheBlock>& blocks,
                  optional_yield y);

    // Retrieve block metadata (single)
    int get(const DoutPrefixProvider* dpp,
            CacheBlock* block,
            optional_yield y);

    // Retrieve multiple blocks (batched)
    int get_batch(const DoutPrefixProvider* dpp,
                  std::vector<CacheBlock>& blocks,
                  optional_yield y);

    // Copy block metadata
    int copy(const DoutPrefixProvider* dpp,
            const CacheBlock* block,
            const std::string& dest_name,
            const std::string& dest_bucket,
            optional_yield y);

    // Delete block metadata
    int del(const DoutPrefixProvider* dpp,
            const CacheBlock* block,
            optional_yield y);

    // Update specific field
    int update_field(const DoutPrefixProvider* dpp,
                    const CacheBlock* block,
                    const std::string& field,
                    const std::string& value,
                    optional_yield y);

    // Remove host from block's host list
    int remove_host(const DoutPrefixProvider* dpp,
                    const CacheBlock* block,
                    const std::string& host,
                    optional_yield y);

  private:
    std::string build_index(const CacheBlock* block) const;
  };

  /**
   * Factory for creating backend-specific implementations
   */

  enum class BackendType {
      Redis,
      FoundationDB
  };

  class BackendFactory {
  public:
    static std::shared_ptr<Backend> create_backend(
      BackendType type,
      const std::string& config);
  };

}} //rgw::d4n