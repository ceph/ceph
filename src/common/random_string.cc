// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <string_view>
#include "auth/Crypto.h"
#include "common/armor.h"
#include "common/ceph_context.h"
#include "common/dout.h"
#include "random_string.h"

int gen_rand_base64(CephContext *cct, char *dest, size_t size) /* size should be the required string size + 1 */
{
  char buf[size];
  char tmp_dest[size + 4]; /* so that there's space for the extra '=' characters, and some */
  int ret;

  cct->random()->get_bytes(buf, sizeof(buf));

  ret = ceph_armor(tmp_dest, &tmp_dest[sizeof(tmp_dest)],
		   (const char *)buf, ((const char *)buf) + ((size - 1) * 3 + 4 - 1) / 4);
  if (ret < 0) {
    lderr(cct) << "ceph_armor failed" << dendl;
    return ret;
  }
  tmp_dest[ret] = '\0';
  memcpy(dest, tmp_dest, size);
  dest[size-1] = '\0';

  return 0;
}

// choose 'size' random characters from the given string table
static void choose_from(CryptoRandom* random, std::string_view table,
                        char *dest, size_t size)
{
  random->get_bytes(dest, size);

  for (size_t i = 0; i < size; i++) {
    auto pos = static_cast<unsigned>(dest[i]);
    dest[i] = table[pos % table.size()];
  }
}


void gen_rand_alphanumeric(CephContext *cct, char *dest, size_t size) /* size should be the required string size + 1 */
{
  // this is basically a modified base64 charset, url friendly
  static constexpr char table[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  choose_from(cct->random(), table, dest, size-1);
  dest[size-1] = 0;
}

std::string gen_rand_alphanumeric(CephContext *cct, size_t size)
{
  std::string str;
  str.resize(size + 1);
  gen_rand_alphanumeric(cct, str.data(), str.size());
  str.pop_back(); // pop the extra \0
  return str;
}

void gen_rand_alphanumeric_lower(CephContext *cct, char *dest, size_t size) /* size should be the required string size + 1 */
{
  static constexpr char table[] = "0123456789abcdefghijklmnopqrstuvwxyz";
  choose_from(cct->random(), table, dest, size-1);
  dest[size-1] = 0;
}

std::string gen_rand_alphanumeric_lower(CephContext *cct, size_t size)
{
  std::string str;
  str.resize(size + 1);
  gen_rand_alphanumeric_lower(cct, str.data(), str.size());
  str.pop_back(); // pop the extra \0
  return str;
}


void gen_rand_alphanumeric_upper(CephContext *cct, char *dest, size_t size) /* size should be the required string size + 1 */
{
  static constexpr char table[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  choose_from(cct->random(), table, dest, size-1);
  dest[size-1] = 0;
}

std::string gen_rand_alphanumeric_upper(CephContext *cct, size_t size)
{
  std::string str;
  str.resize(size + 1);
  gen_rand_alphanumeric_upper(cct, str.data(), str.size());
  str.pop_back(); // pop the extra \0
  return str;
}


void gen_rand_alphanumeric_no_underscore(CephContext *cct, char *dest, size_t size) /* size should be the required string size + 1 */
{
  static constexpr char table[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-.";
  choose_from(cct->random(), table, dest, size-1);
  dest[size-1] = 0;
}

std::string gen_rand_alphanumeric_no_underscore(CephContext *cct, size_t size)
{
  std::string str;
  str.resize(size + 1);
  gen_rand_alphanumeric_no_underscore(cct, str.data(), str.size());
  str.pop_back(); // pop the extra \0
  return str;
}


void gen_rand_alphanumeric_plain(CephContext *cct, char *dest, size_t size) /* size should be the required string size + 1 */
{
  static constexpr char table[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  choose_from(cct->random(), table, dest, size-1);
  dest[size-1] = 0;
}

std::string gen_rand_alphanumeric_plain(CephContext *cct, size_t size)
{
  std::string str;
  str.resize(size + 1);
  gen_rand_alphanumeric_plain(cct, str.data(), str.size());
  str.pop_back(); // pop the extra \0
  return str;
}

void gen_rand_numeric(CephContext *cct, char *dest, size_t size) /* size should be the required string size + 1 */
{
  static constexpr char table[] = "0123456789";
  choose_from(cct->random(), table, dest, size-1);
  dest[size-1] = 0;
}

std::string gen_rand_numeric(CephContext *cct, size_t size)
{
  std::string str;
  str.resize(size + 1);
  gen_rand_numeric(cct, str.data(), str.size());
  str.pop_back(); // pop the extra \0
  return str;
}
