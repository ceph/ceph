// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <errno.h>
#include <stdlib.h>

#include <ctime>

#include "common/split.h"
#include "rgw_torrent.h"
#include "rgw_sal.h"
#include "rgw_sal_rados.h"
#include "include/str_list.h"
#include "include/rados/librados.hpp"

#include "services/svc_sys_obj.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;
using namespace librados;
using namespace boost;
using ceph::crypto::SHA1;

seed::seed()
{
  seed::info.piece_length = 0;
  seed::info.len = 0;
  sha_len = 0;
  is_torrent = false; 
}

seed::~seed()
{
  seed::info.sha1_bl.clear();
  bl.clear();
  s = NULL;
  driver = NULL;
}

void seed::init(req_state *_req, rgw::sal::Driver* _driver)
{
  s = _req;
  driver = _driver;
}

int rgw_read_torrent_file(const DoutPrefixProvider* dpp,
                          rgw::sal::Object* object,
                          ceph::bufferlist &bl,
                          optional_yield y)
{
  bufferlist infobl;
  int r = object->get_torrent_info(dpp, y, infobl);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: read_torrent_info failed: " << r << dendl;
    return r;
  }

  // add other fields from config
  auto& conf = dpp->get_cct()->_conf;

  TorrentBencode benc;
  benc.bencode_dict(bl);

  auto trackers = ceph::split(conf->rgw_torrent_tracker, ",");
  if (auto i = trackers.begin(); i != trackers.end()) {
    benc.bencode_key(ANNOUNCE, bl);
    benc.bencode_key(*i, bl);

    benc.bencode_key(ANNOUNCE_LIST, bl);
    benc.bencode_list(bl);
    for (; i != trackers.end(); ++i) {
      benc.bencode_list(bl);
      benc.bencode_key(*i, bl);
      benc.bencode_end(bl);
    }
    benc.bencode_end(bl);
  }

  std::string_view comment = conf->rgw_torrent_comment;
  if (!comment.empty()) {
    benc.bencode(COMMENT, comment, bl);
  }
  std::string_view create_by = conf->rgw_torrent_createby;
  if (!create_by.empty()) {
    benc.bencode(CREATED_BY, create_by, bl);
  }
  std::string_view encoding = conf->rgw_torrent_encoding;
  if (!encoding.empty()) {
    benc.bencode(ENCODING, encoding, bl);
  }

  // append the info stored in the object
  bl.append(std::move(infobl));
  return 0;
}

int seed::get_torrent_file(rgw::sal::Object* object,
                           uint64_t &total_len,
                           ceph::bufferlist &bl_data,
                           rgw_obj &obj)
{
  /* add other field if config is set */
  dencode.bencode_dict(bl);
  set_announce();
  if (!comment.empty())
  {
    dencode.bencode(COMMENT, comment, bl);
  }
  if (!create_by.empty())
  {
    dencode.bencode(CREATED_BY, create_by, bl);
  }
  if (!encoding.empty())
  {
    dencode.bencode(ENCODING, encoding, bl);
  }

  string oid, key;
  get_obj_bucket_and_oid_loc(obj, oid, key);
  ldpp_dout(s, 20) << "NOTICE: head obj oid= " << oid << dendl;

  const set<string> obj_key{RGW_OBJ_TORRENT};
  map<string, bufferlist> m;
  const int r = object->omap_get_vals_by_keys(s, oid, obj_key, &m);
  if (r < 0) {
    ldpp_dout(s, 0) << "ERROR: omap_get_vals_by_keys failed: " << r << dendl;
    return r;
  }
  if (m.size() != 1) {
    ldpp_dout(s, 0) << "ERROR: omap key " RGW_OBJ_TORRENT " not found" << dendl;
    return -EINVAL;
  }
  bl.append(std::move(m.begin()->second));
  dencode.bencode_end(bl);

  bl_data = bl;
  total_len = bl.length();
  return 0;
}

bool seed::get_flag()
{
  return is_torrent;
}

void seed::update(bufferlist &bl)
{
  if (!is_torrent)
  {
    return;
  }
  info.len += bl.length();
  sha1(&h, bl, bl.length());
}

int seed::complete(optional_yield y)
{
  uint64_t remain = info.len%info.piece_length;
  uint8_t  remain_len = ((remain > 0)? 1 : 0);
  sha_len = (info.len/info.piece_length + remain_len)*CEPH_CRYPTO_SHA1_DIGESTSIZE;

  int ret = 0;
   /* produce torrent data */
  do_encode();

  /* save torrent data into OMAP */
  ret = save_torrent_file(y);
  if (0 != ret)
  {
    ldpp_dout(s, 0) << "ERROR: failed to save_torrent_file() ret= "<< ret << dendl;
    return ret;
  }

  return 0;
}

off_t seed::get_data_len()
{
  return info.len;
}

void seed::set_create_date(ceph::real_time& value)
{
  utime_t date = ceph::real_clock::to_timespec(value);
  create_date = date.sec();
}

void seed::set_info_pieces(char *buff)
{
  info.sha1_bl.append(buff, CEPH_CRYPTO_SHA1_DIGESTSIZE);
}

void seed::set_info_name(const string& value)
{
  info.name = value;
}

void seed::sha1(SHA1 *h, bufferlist &bl, off_t bl_len)
{
  off_t num = bl_len/info.piece_length;
  off_t remain = 0;
  remain = bl_len%info.piece_length;

  char *pstr = bl.c_str();
  char sha[25];

  /* get sha1 */
  for (off_t i = 0; i < num; i++)
  {
    // FIPS zeroization audit 20191116: this memset is not intended to
    // wipe out a secret after use.
    memset(sha, 0x00, sizeof(sha));
    h->Update((unsigned char *)pstr, info.piece_length);
    h->Final((unsigned char *)sha);
    set_info_pieces(sha);
    pstr += info.piece_length;
  }

  /* process remain */
  if (0 != remain)
  {
    // FIPS zeroization audit 20191116: this memset is not intended to
    // wipe out a secret after use.
    memset(sha, 0x00, sizeof(sha));
    h->Update((unsigned char *)pstr, remain);
    h->Final((unsigned char *)sha);
    set_info_pieces(sha);
  }
  ::ceph::crypto::zeroize_for_security(sha, sizeof(sha));
}

int seed::get_params()
{
  is_torrent = true;
  info.piece_length = g_conf()->rgw_torrent_sha_unit;
  create_by = g_conf()->rgw_torrent_createby;
  encoding = g_conf()->rgw_torrent_encoding;
  origin = g_conf()->rgw_torrent_origin;
  comment = g_conf()->rgw_torrent_comment;
  announce = g_conf()->rgw_torrent_tracker;

  /* tracker and tracker list is empty, set announce to origin */
  if (announce.empty() && !origin.empty())
  {
    announce = origin;
  }

  return 0;
}

void seed::set_announce()
{
  list<string> announce_list;  // used to get announce list from conf
  get_str_list(announce, ",", announce_list);

  if (announce_list.empty())
  {
    ldpp_dout(s, 5) << "NOTICE: announce_list is empty " << dendl;    
    return;
  }

  list<string>::iterator iter = announce_list.begin();
  dencode.bencode_key(ANNOUNCE, bl);
  dencode.bencode_key((*iter), bl);

  dencode.bencode_key(ANNOUNCE_LIST, bl);
  dencode.bencode_list(bl);
  for (; iter != announce_list.end(); ++iter)
  {
    dencode.bencode_list(bl);
    dencode.bencode_key((*iter), bl);
    dencode.bencode_end(bl);
  }
  dencode.bencode_end(bl);
}

void seed::do_encode()
{ 
  /*Only encode create_date and sha1 info*/
  /*Other field will be added if confi is set when run get torrent*/
  dencode.bencode(CREATION_DATE, create_date, bl);

  dencode.bencode_key(INFO_PIECES, bl);
  dencode.bencode_dict(bl);  
  dencode.bencode(LENGTH, info.len, bl);
  dencode.bencode(NAME, info.name, bl);
  dencode.bencode(PIECE_LENGTH, info.piece_length, bl);

  char info_sha[100] = { 0 };
  sprintf(info_sha, "%" PRIu64, sha_len);
  string sha_len_str = info_sha;
  dencode.bencode_key(PIECES, bl);
  bl.append(sha_len_str.c_str(), sha_len_str.length());
  bl.append(':');
  bl.append(info.sha1_bl.c_str(), sha_len);
  dencode.bencode_end(bl);
}

int seed::save_torrent_file(optional_yield y)
{
  int op_ret = 0;
  string key = RGW_OBJ_TORRENT;

  op_ret = s->object->omap_set_val_by_key(s, key, bl, false, y);
  if (op_ret < 0)
  {
    ldpp_dout(s, 0) << "ERROR: failed to omap_set() op_ret = " << op_ret << dendl;
    return op_ret;
  }

  return op_ret;
}

RGWPutObj_Torrent::RGWPutObj_Torrent(rgw::sal::DataProcessor* next,
                                     size_t max_len, size_t piece_len)
  : Pipe(next), max_len(max_len), piece_len(piece_len)
{
}

int RGWPutObj_Torrent::process(bufferlist&& data, uint64_t logical_offset)
{
  if (!data.length()) { // done
    if (piece_offset) { // hash the remainder
      char out[SHA1::digest_size];
      digest.Final(reinterpret_cast<unsigned char*>(out));
      piece_hashes.append(out, sizeof(out));
      piece_count++;
    }
    return Pipe::process(std::move(data), logical_offset);
  }

  len += data.length();
  if (len >= max_len) {
    // enforce the maximum object size; stop calculating and buffering hashes
    piece_hashes.clear();
    piece_offset = 0;
    piece_count = 0;
    return Pipe::process(std::move(data), logical_offset);
  }

  auto p = data.begin();
  while (!p.end()) {
    // feed each buffer segment through sha1
    uint32_t want = piece_len - piece_offset;
    const char* buf = nullptr;
    size_t bytes = p.get_ptr_and_advance(want, &buf);
    digest.Update(reinterpret_cast<const unsigned char*>(buf), bytes);
    piece_offset += bytes;

    // record the hash digest at each piece boundary
    if (bytes == want) {
      char out[SHA1::digest_size];
      digest.Final(reinterpret_cast<unsigned char*>(out));
      digest.Restart();
      piece_hashes.append(out, sizeof(out));
      piece_count++;
      piece_offset = 0;
    }
  }

  return Pipe::process(std::move(data), logical_offset);
}

bufferlist RGWPutObj_Torrent::bencode_torrent(std::string_view filename) const
{
  bufferlist bl;
  if (len >= max_len) {
    return bl;
  }

  /*Only encode create_date and sha1 info*/
  /*Other field will be added if config is set when run get torrent*/
  TorrentBencode benc;
  benc.bencode(CREATION_DATE, std::time(nullptr), bl);

  benc.bencode_key(INFO_PIECES, bl);
  benc.bencode_dict(bl);
  benc.bencode(LENGTH, len, bl);
  benc.bencode(NAME, filename, bl);
  benc.bencode(PIECE_LENGTH, piece_len, bl);

  benc.bencode_key(PIECES, bl);
  bl.append(std::to_string(piece_count));
  bl.append(':');
  bl.append(piece_hashes);
  benc.bencode_end(bl);

  return bl;
}
