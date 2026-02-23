// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_LIBRBD_CRYPTO_LUKS_HEADER_H
#define CEPH_LIBRBD_CRYPTO_LUKS_HEADER_H

#include <libcryptsetup.h>
#include "common/ceph_context.h"
#include "common/ceph_json.h"
#include "include/buffer.h"

namespace librbd {
namespace crypto {
namespace luks {

namespace token {
  static const char* const TYPE_AEAD = "ceph-aead";
  static const char* const KEY_META_SIZE = "meta_size";
  static const char* const KEY_ALG = "alg";
}

struct AEADToken {
  uint32_t meta_size = 0;
  std::string alg;

  void encode_json(ceph::Formatter *f) const {
    f->dump_string("type", token::TYPE_AEAD);
    f->open_array_section("keyslots");
    f->close_section();
    f->dump_unsigned(token::KEY_META_SIZE, meta_size);
    f->dump_string(token::KEY_ALG, alg);
  }

  void decode_json(JSONObj *obj) {
    JSONDecoder::decode_json(token::KEY_META_SIZE, meta_size, obj);
    JSONDecoder::decode_json(token::KEY_ALG, alg, obj);
  }
};

class Header {
public:
    Header(CephContext* cct);
    ~Header();
    int init();

    int write(const ceph::bufferlist& bl);
    ssize_t read(ceph::bufferlist* bl);

    int format(const char* type, const char* alg, const char* key,
               size_t key_size, const char* cipher_mode, uint32_t sector_size,
               uint32_t data_alignment, bool insecure_fast_mode);
    int add_keyslot(const char* passphrase, size_t passphrase_size);
    int token_update(const char* identifier, const char* json_data);
    int token_get(const char* identifier, const char** json_data);
    int load(const char* type);
    int read_volume_key(const char* passphrase, size_t passphrase_size,
                        char* volume_key, size_t* volume_key_size);

    int get_sector_size();
    uint64_t get_data_offset();
    const char* get_cipher();
    const char* get_cipher_mode();
    const char* get_format_name();

private:
    void libcryptsetup_log(int level, const char* msg);
    static void libcryptsetup_log_wrapper(int level, const char* msg,
                                          void* header);
    int find_token_slot(const char* identifier) const; 

    CephContext* m_cct;
    int m_fd;
    struct crypt_device *m_cd;
};

} // namespace luks
} // namespace crypto
} // namespace librbd

#endif // CEPH_LIBRBD_CRYPTO_LUKS_HEADER_H
