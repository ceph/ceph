#include "kvrgw_c_api.h"

#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define CHECK(ec, what)                                                       \
  do {                                                                        \
    if ((ec) != KVRGW_ERR_OK) {                                               \
      fprintf(stderr, "FAIL %s ec=%d\n", (what), (int)(ec));                  \
      if (h != NULL) {                                                        \
        kvrgw_stop(h);                                                        \
      }                                                                       \
      return 1;                                                               \
    }                                                                         \
  } while (0)

static int kv_has(const kvrgw_kv* md, uint32_t n, const char* k, const char* v)
{
  const size_t kn = strlen(k);
  const size_t vn = strlen(v);
  for (uint32_t i = 0; i < n; ++i) {
    if (md[i].key_len == kn && md[i].value_len == vn &&
        memcmp(md[i].key, k, kn) == 0 && memcmp(md[i].value, v, vn) == 0) {
      return 1;
    }
  }
  return 0;
}

int main(void)
{
  KvRgwHandle* h = NULL;
  const char data_root[] = "/tmp/kvrgw-cabi-data";
  mkdir(data_root, 0755);
  kvrgw_err_t ec = kvrgw_start(data_root, sizeof(data_root) - 1, &h);
  CHECK(ec, "kvrgw_start");

  char tenant[64];
  int n = snprintf(tenant, sizeof(tenant), "cabi-%d", (int)getpid());
  uint32_t tenant_id = 0;
  ec = kvrgw_add_tenant(h, tenant, (size_t)n, &tenant_id);
  if (ec == KVRGW_ERR_BUCKET_ALREADY_EXISTS) {
    int exists = 0;
    ec = kvrgw_resolve_tenant(h, tenant, (size_t)n, &exists, &tenant_id);
    CHECK(ec, "kvrgw_resolve_tenant");
    if (!exists) {
      fprintf(stderr, "FAIL tenant missing after already-exists\n");
      kvrgw_stop(h);
      return 1;
    }
  } else {
    CHECK(ec, "kvrgw_add_tenant");
  }

  const char bucket[] = "cabi-bucket";
  ec = kvrgw_create_bucket(h, tenant_id, bucket, sizeof(bucket) - 1);
  CHECK(ec, "kvrgw_create_bucket");

  {
    kvrgw_list_bkt_entry bents[1];
    uint32_t bcount = 0;
    char barena[256];
    char btok[64];
    size_t btok_len = 0;
    ec = kvrgw_list_buckets(h, tenant_id, NULL, 0, NULL, 0, 0, bents, 1, &bcount,
                            barena, sizeof(barena), btok, sizeof(btok), &btok_len);
    if (ec != KVRGW_ERR_INVALID_ARGUMENT) {
      fprintf(stderr, "FAIL list_buckets max=0 ec=%d want=%d\n", (int)ec,
              KVRGW_ERR_INVALID_ARGUMENT);
      kvrgw_stop(h);
      return 1;
    }
    bcount = 0;
    btok_len = 0;
    ec = kvrgw_list_buckets(h, tenant_id, NULL, 0, NULL, 0, 1, bents, 1, &bcount,
                            barena, sizeof(barena), btok, sizeof(btok), &btok_len);
    CHECK(ec, "kvrgw_list_buckets");
    if (bcount != 1) {
      fprintf(stderr, "FAIL list_buckets count=%u want=1\n", bcount);
      kvrgw_stop(h);
      return 1;
    }
  }

  const char key[] = "obj1";
  const uint8_t body[] = "hello-cabi";
  kvrgw_kv md_in[2] = {
      {.key = "color", .key_len = 5, .value = "blue", .value_len = 4},
      {.key = "owner", .key_len = 5, .value = "kv", .value_len = 2},
  };
  char etag[48];
  char version_id[9];
  etag[0] = '\0';
  version_id[0] = '\0';
  ec = kvrgw_put_object(h, tenant_id, bucket, sizeof(bucket) - 1, key, sizeof(key) - 1,
                        body, sizeof(body) - 1, NULL, 0, sizeof(body) - 1, NULL, 0, NULL, 0,
                        NULL, 0, md_in, 2, etag, sizeof(etag), version_id, sizeof(version_id));
  CHECK(ec, "kvrgw_put_object");

  kvrgw_object_meta meta;
  kvrgw_kv md_out[8];
  char meta_arena[512];
  uint32_t md_count = 0;
  memset(&meta, 0, sizeof(meta));
  ec = kvrgw_head_object(h, tenant_id, bucket, sizeof(bucket) - 1, key, sizeof(key) - 1,
                         NULL, 0, &meta, md_out, 8, &md_count, meta_arena, sizeof(meta_arena));
  CHECK(ec, "kvrgw_head_object");
  if (meta.size != sizeof(body) - 1) {
    fprintf(stderr, "FAIL head size got=%llu want=%zu\n",
            (unsigned long long)meta.size, sizeof(body) - 1);
    kvrgw_stop(h);
    return 1;
  }
  if (md_count != 2 || !kv_has(md_out, md_count, "color", "blue") ||
      !kv_has(md_out, md_count, "owner", "kv")) {
    fprintf(stderr, "FAIL head metadata count=%u\n", md_count);
    kvrgw_stop(h);
    return 1;
  }

  uint8_t got[64];
  size_t got_len = 0;
  md_count = 0;
  memset(&meta, 0, sizeof(meta));
  ec = kvrgw_get_object(h, tenant_id, bucket, sizeof(bucket) - 1, key, sizeof(key) - 1,
                        NULL, 0, NULL, &meta, got, sizeof(got), &got_len,
                        md_out, 8, &md_count, meta_arena, sizeof(meta_arena));
  CHECK(ec, "kvrgw_get_object");
  if (got_len != sizeof(body) - 1 || memcmp(got, body, got_len) != 0) {
    fprintf(stderr, "FAIL get body mismatch len=%zu\n", got_len);
    kvrgw_stop(h);
    return 1;
  }
  if (md_count != 2 || !kv_has(md_out, md_count, "color", "blue") ||
      !kv_has(md_out, md_count, "owner", "kv")) {
    fprintf(stderr, "FAIL get metadata count=%u\n", md_count);
    kvrgw_stop(h);
    return 1;
  }

  kvrgw_byte_range rng = {.start = 0, .end = 4, .from_end = 0, .end_unbounded = 0};
  got_len = 0;
  memset(&meta, 0, sizeof(meta));
  ec = kvrgw_get_object(h, tenant_id, bucket, sizeof(bucket) - 1, key, sizeof(key) - 1,
                        NULL, 0, &rng, &meta, got, sizeof(got), &got_len,
                        NULL, 0, NULL, NULL, 0);
  CHECK(ec, "kvrgw_get_object range");
  if (got_len != 5 || memcmp(got, "hello", 5) != 0) {
    fprintf(stderr, "FAIL range get len=%zu\n", got_len);
    kvrgw_stop(h);
    return 1;
  }

  kvrgw_list_obj_entry entries[8];
  uint32_t entries_count = 0;
  uint32_t prefix_offs[8];
  uint32_t prefix_lens[8];
  uint32_t prefixes_count = 0;
  char arena[4096];
  int truncated = 0;
  char next_token[256];
  size_t next_token_len = 0;
  ec = kvrgw_list_objects(h, tenant_id, bucket, sizeof(bucket) - 1, NULL, 0, NULL, 0, 1000,
                          NULL, 0, NULL, 0, entries, 8, &entries_count, prefix_offs, prefix_lens, 8,
                          &prefixes_count, arena, sizeof(arena), &truncated, next_token,
                          sizeof(next_token), &next_token_len);
  CHECK(ec, "kvrgw_list_objects");
  if (entries_count != 1) {
    fprintf(stderr, "FAIL list count=%u want=1\n", entries_count);
    kvrgw_stop(h);
    return 1;
  }

  int created_dm = 0;
  uint32_t dm_vid = 0;
  ec = kvrgw_delete_object(h, tenant_id, bucket, sizeof(bucket) - 1, key, sizeof(key) - 1,
                           NULL, 0, 0, 0, 0, &created_dm, &dm_vid);
  CHECK(ec, "kvrgw_delete_object");

  memset(&meta, 0, sizeof(meta));
  ec = kvrgw_head_object(h, tenant_id, bucket, sizeof(bucket) - 1, key, sizeof(key) - 1,
                         NULL, 0, &meta, NULL, 0, NULL, NULL, 0);
  if (ec != KVRGW_ERR_NO_SUCH_KEY) {
    fprintf(stderr, "FAIL head after delete ec=%d want=%d\n", (int)ec, KVRGW_ERR_NO_SUCH_KEY);
    kvrgw_stop(h);
    return 1;
  }

  kvrgw_stop(h);
  printf("kvrgw_c_abi_test PASS tenant_id=%u etag=%s\n", tenant_id, etag);
  return 0;
}
