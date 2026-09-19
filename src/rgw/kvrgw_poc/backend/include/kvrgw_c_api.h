#ifndef KVRGW_C_API_H
#define KVRGW_C_API_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef int32_t kvrgw_err_t;

enum {
  KVRGW_ERR_OK = 0,
  KVRGW_ERR_NO_SUCH_KEY = 100,
  KVRGW_ERR_NO_SUCH_BUCKET = 101,
  KVRGW_ERR_BUCKET_ALREADY_EXISTS = 102,
  KVRGW_ERR_BUCKET_NOT_EMPTY = 103,
  KVRGW_ERR_PRECONDITION_FAILED = 104,
  KVRGW_ERR_ACCESS_DENIED = 105,
  KVRGW_ERR_INVALID_ARGUMENT = 106,
  KVRGW_ERR_INVALID_BUCKET_NAME = 107,
  KVRGW_ERR_NO_SUCH_VERSION = 108,
  KVRGW_ERR_NO_SUCH_TENANT = 109,
  KVRGW_ERR_INVALID_RANGE = 110,
  KVRGW_ERR_INVALID_REQUEST = 111,
  KVRGW_ERR_INVALID_TAG = 112,
  KVRGW_ERR_FDB_CONFLICT = 200,
  KVRGW_ERR_FDB_PROCESS_BEHIND = 201,
  KVRGW_ERR_FDB_FUTURE_VERSION = 202,
  KVRGW_ERR_FDB_TRANSACTION_TOO_OLD = 203,
  KVRGW_ERR_FDB_NOT_COMMITTED = 204,
  KVRGW_ERR_FDB_COMMIT_UNKNOWN = 205,
  KVRGW_ERR_FDB_CLUSTER_VERSION_CHANGED = 206,
  KVRGW_ERR_FDB_KEY_TOO_LARGE = 300,
  KVRGW_ERR_FDB_VALUE_TOO_LARGE = 301,
  KVRGW_ERR_FDB_TRANSACTION_TOO_LARGE = 302,
  KVRGW_ERR_INTERNAL = 400,
  KVRGW_ERR_CORRUPT_VALUE = 401,
  KVRGW_ERR_BUCKET_ID_MISMATCH = 402,
  KVRGW_ERR_VALUE_TOO_LARGE = 403,
  KVRGW_ERR_TRANSACTION_CONFLICT = 404,
  KVRGW_ERR_MAX_RETRIES_EXCEEDED = 405
};

enum {
  KVRGW_VERSIONING_DISABLED = 0,
  KVRGW_VERSIONING_ENABLED = 1,
  KVRGW_VERSIONING_SUSPENDED = 2
};

enum { KVRGW_MAX_LIST_KEYS = 1000 };
enum { KVRGW_MAX_LIST_BUCKETS = 10000 };

typedef struct KvRgwHandle KvRgwHandle;

typedef struct {
  const char* data;
  size_t len;
} kvrgw_buf;

typedef struct {
  const char* key;
  size_t key_len;
  const char* value;
  size_t value_len;
} kvrgw_kv;

typedef struct {
  int64_t start;
  int64_t end;
  int from_end;
  int end_unbounded;
} kvrgw_byte_range;

typedef struct {
  char etag[48];
  uint64_t size;
  int64_t last_modified_unix;
  char content_type[256];
  char version_id[9];
  uint32_t tags_count;
  char error_detail[128];
} kvrgw_object_meta;

typedef struct {
  uint32_t key_off;
  uint32_t key_len;
  uint64_t size;
  uint32_t etag_off;
  uint32_t etag_len;
  int64_t last_modified_unix;
} kvrgw_list_obj_entry;

typedef struct {
  uint32_t name_off;
  uint32_t name_len;
  int64_t created_at_unix;
} kvrgw_list_bkt_entry;

typedef struct {
  uint32_t key_off;
  uint32_t key_len;
  uint32_t version_id;
  int is_latest;
  int is_delete_marker;
  uint64_t size;
  uint32_t etag_off;
  uint32_t etag_len;
  int64_t last_modified_unix;
} kvrgw_list_ver_entry;

typedef struct {
  uint32_t key_off;
  uint32_t key_len;
  int status; /* 0 deleted, 1 error */
  uint32_t err_code_off;
  uint32_t err_code_len;
  uint32_t err_msg_off;
  uint32_t err_msg_len;
  uint32_t version_id_off;
  uint32_t version_id_len;
  int created_dm;
  uint32_t dm_version_id;
} kvrgw_del_multi_out;

typedef struct {
  kvrgw_buf src_bucket;
  kvrgw_buf src_key;
  kvrgw_buf dst_bucket;
  kvrgw_buf dst_key;
  kvrgw_buf src_version_id;
  kvrgw_buf if_match;
  kvrgw_buf if_none_match;
  kvrgw_buf dst_if_match;
  kvrgw_buf dst_if_none_match;
  kvrgw_buf content_type;
  int replace_metadata;
  const kvrgw_kv* metadata;
  size_t metadata_count;
  int replace_tags;
  const kvrgw_kv* tags;
  size_t tags_count;
} kvrgw_copy_args;

kvrgw_err_t kvrgw_start(const char* data_root, size_t data_root_len, KvRgwHandle** out);
void kvrgw_stop(KvRgwHandle* h);

kvrgw_err_t kvrgw_add_tenant(KvRgwHandle* h, const char* name, size_t name_len, uint32_t* out_id);
kvrgw_err_t kvrgw_resolve_tenant(KvRgwHandle* h, const char* name, size_t name_len,
                                 int* out_exists, uint32_t* out_id);

kvrgw_err_t kvrgw_create_bucket(KvRgwHandle* h, uint32_t tenant_id,
                                const char* bucket, size_t bucket_len);
kvrgw_err_t kvrgw_delete_bucket(KvRgwHandle* h, uint32_t tenant_id,
                                const char* bucket, size_t bucket_len);
kvrgw_err_t kvrgw_bucket_exists(KvRgwHandle* h, uint32_t tenant_id,
                                const char* bucket, size_t bucket_len,
                                int* out_exists, uint64_t* out_id);
kvrgw_err_t kvrgw_bucket_exists_cached(KvRgwHandle* h, uint32_t tenant_id,
                                       const char* bucket, size_t bucket_len,
                                       int* out_exists, uint64_t* out_id);

kvrgw_err_t kvrgw_put_bucket_versioning(KvRgwHandle* h, uint32_t tenant_id,
                                        const char* bucket, size_t bucket_len, uint8_t state);
kvrgw_err_t kvrgw_get_bucket_versioning(KvRgwHandle* h, uint32_t tenant_id,
                                        const char* bucket, size_t bucket_len, uint8_t* out_state);

kvrgw_err_t kvrgw_put_bucket_policy(KvRgwHandle* h, uint32_t tenant_id,
                                    const char* bucket, size_t bucket_len,
                                    const char* policy, size_t policy_len);
kvrgw_err_t kvrgw_get_bucket_policy(KvRgwHandle* h, uint32_t tenant_id,
                                    const char* bucket, size_t bucket_len,
                                    char* buf, size_t buf_cap, size_t* out_len);
kvrgw_err_t kvrgw_delete_bucket_policy(KvRgwHandle* h, uint32_t tenant_id,
                                       const char* bucket, size_t bucket_len);

kvrgw_err_t kvrgw_put_object(KvRgwHandle* h, uint32_t tenant_id,
                             const char* bucket, size_t bucket_len,
                             const char* key, size_t key_len,
                             const uint8_t* data, size_t data_len,
                             const char* content_type, size_t content_type_len,
                             uint64_t estimated_size,
                             const char* if_match, size_t if_match_len,
                             const char* if_none_match, size_t if_none_match_len,
                             const kvrgw_kv* tags, size_t tags_count,
                             const kvrgw_kv* metadata, size_t metadata_count,
                             char* etag_out, size_t etag_cap,
                             char* version_id_out, size_t version_id_cap);

kvrgw_err_t kvrgw_get_object(KvRgwHandle* h, uint32_t tenant_id,
                             const char* bucket, size_t bucket_len,
                             const char* key, size_t key_len,
                             const char* version_id, size_t version_id_len,
                             const kvrgw_byte_range* range,
                             kvrgw_object_meta* meta,
                             uint8_t* body, size_t body_cap, size_t* body_len,
                             kvrgw_kv* metadata, uint32_t metadata_cap, uint32_t* metadata_count,
                             char* meta_arena, size_t meta_arena_cap);

kvrgw_err_t kvrgw_head_object(KvRgwHandle* h, uint32_t tenant_id,
                              const char* bucket, size_t bucket_len,
                              const char* key, size_t key_len,
                              const char* version_id, size_t version_id_len,
                              kvrgw_object_meta* meta,
                              kvrgw_kv* metadata, uint32_t metadata_cap, uint32_t* metadata_count,
                              char* meta_arena, size_t meta_arena_cap);

kvrgw_err_t kvrgw_delete_object(KvRgwHandle* h, uint32_t tenant_id,
                                const char* bucket, size_t bucket_len,
                                const char* key, size_t key_len,
                                const char* if_match, size_t if_match_len,
                                int64_t if_match_mtime, int64_t if_match_size,
                                int has_if_match_size,
                                int* created_dm, uint32_t* dm_version_id);

kvrgw_err_t kvrgw_delete_object_version(KvRgwHandle* h, uint32_t tenant_id,
                                        const char* bucket, size_t bucket_len,
                                        const char* key, size_t key_len,
                                        const char* version_id, size_t version_id_len,
                                        const char* if_match, size_t if_match_len,
                                        int64_t if_match_mtime, int64_t if_match_size,
                                        int has_if_match_size);

kvrgw_err_t kvrgw_delete_multi(KvRgwHandle* h, uint32_t tenant_id,
                               const char* bucket, size_t bucket_len,
                               const kvrgw_buf* keys, size_t keys_count,
                               const kvrgw_kv* objects, size_t objects_count,
                               kvrgw_del_multi_out* outs, uint32_t outs_cap, uint32_t* outs_count,
                               char* arena, size_t arena_cap);

kvrgw_err_t kvrgw_list_buckets(KvRgwHandle* h, uint32_t tenant_id,
                               const char* prefix, size_t prefix_len,
                               const char* continuation, size_t continuation_len,
                               uint32_t max_buckets,
                               kvrgw_list_bkt_entry* entries, uint32_t entries_cap,
                               uint32_t* entries_count,
                               char* arena, size_t arena_cap,
                               char* next_token, size_t next_token_cap, size_t* next_token_len);

kvrgw_err_t kvrgw_list_objects(KvRgwHandle* h, uint32_t tenant_id,
                               const char* bucket, size_t bucket_len,
                               const char* prefix, size_t prefix_len,
                               const char* delimiter, size_t delimiter_len,
                               uint32_t max_keys,
                               const char* continuation, size_t continuation_len,
                               const char* marker, size_t marker_len,
                               kvrgw_list_obj_entry* entries, uint32_t entries_cap,
                               uint32_t* entries_count,
                               uint32_t* prefix_offs, uint32_t* prefix_lens,
                               uint32_t prefixes_cap, uint32_t* prefixes_count,
                               char* arena, size_t arena_cap,
                               int* is_truncated,
                               char* next_token, size_t next_token_cap, size_t* next_token_len);

kvrgw_err_t kvrgw_list_object_versions(KvRgwHandle* h, uint32_t tenant_id,
                                       const char* bucket, size_t bucket_len,
                                       const char* prefix, size_t prefix_len,
                                       uint32_t max_keys,
                                       const char* key_marker, size_t key_marker_len,
                                       uint32_t version_id_marker,
                                       kvrgw_list_ver_entry* entries, uint32_t entries_cap,
                                       uint32_t* entries_count,
                                       char* arena, size_t arena_cap,
                                       int* is_truncated,
                                       char* next_key, size_t next_key_cap, size_t* next_key_len,
                                       uint32_t* next_version_id);

kvrgw_err_t kvrgw_copy_object(KvRgwHandle* h, uint32_t tenant_id, const kvrgw_copy_args* args,
                              char* etag_out, size_t etag_cap,
                              int64_t* last_modified_unix,
                              char* version_id_out, size_t version_id_cap,
                              char* src_version_id_out, size_t src_version_id_cap);

kvrgw_err_t kvrgw_put_object_tagging(KvRgwHandle* h, uint32_t tenant_id,
                                     const char* bucket, size_t bucket_len,
                                     const char* key, size_t key_len,
                                     const kvrgw_kv* tags, size_t tags_count);
kvrgw_err_t kvrgw_get_object_tagging(KvRgwHandle* h, uint32_t tenant_id,
                                     const char* bucket, size_t bucket_len,
                                     const char* key, size_t key_len,
                                     kvrgw_kv* tags, uint32_t tags_cap, uint32_t* tags_count,
                                     char* arena, size_t arena_cap);
kvrgw_err_t kvrgw_delete_object_tagging(KvRgwHandle* h, uint32_t tenant_id,
                                        const char* bucket, size_t bucket_len,
                                        const char* key, size_t key_len);

#ifdef __cplusplus
}
#endif

#endif /* KVRGW_C_API_H */
