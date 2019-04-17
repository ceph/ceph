// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>
#include <stdlib.h>

#include <sstream>

#include "rgw_torrent.h"
#include "include/str_list.h"
#include "include/rados/librados.hpp"

#include "services/svc_sys_obj.h"

#define dout_subsys ceph_subsys_rgw

using ceph::crypto::MD5;
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
  store = NULL;
}

void seed::init(struct req_state *p_req, RGWRados *p_store)
{
  s = p_req;
  store = p_store;
}

int seed::get_torrent_file(RGWRados::Object::Read &read_op,
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
  ldout(s->cct, 20) << "NOTICE: head obj oid= " << oid << dendl;

  const set<string> obj_key{RGW_OBJ_TORRENT};
  map<string, bufferlist> m;
  const int r = read_op.state.cur_ioctx->omap_get_vals_by_keys(oid, obj_key, &m);
  if (r < 0) {
    ldout(s->cct, 0) << "ERROR: omap_get_vals_by_keys failed: " << r << dendl;
    return r;
  }
  if (m.size() != 1) {
    ldout(s->cct, 0) << "ERROR: omap key " RGW_OBJ_TORRENT " not found" << dendl;
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

int seed::complete()
{
  uint64_t remain = info.len%info.piece_length;
  uint8_t  remain_len = ((remain > 0)? 1 : 0);
  sha_len = (info.len/info.piece_length + remain_len)*CEPH_CRYPTO_SHA1_DIGESTSIZE;

  int ret = 0;
   /* produce torrent data */
  do_encode();

  /* save torrent data into OMAP */
  ret = save_torrent_file();
  if (0 != ret)
  {
    ldout(s->cct, 0) << "ERROR: failed to save_torrent_file() ret= "<< ret << dendl;
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
    memset(sha, 0x00, sizeof(sha));
    h->Update((unsigned char *)pstr, info.piece_length);
    h->Final((unsigned char *)sha);
    set_info_pieces(sha);
    pstr += info.piece_length;
  }

  /* process remain */
  if (0 != remain)
  {
    memset(sha, 0x00, sizeof(sha));
    h->Update((unsigned char *)pstr, remain);
    h->Final((unsigned char *)sha);
    set_info_pieces(sha);
  }
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
    ldout(s->cct, 5) << "NOTICE: announce_list is empty " << dendl;    
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

int seed::save_torrent_file()
{
  int op_ret = 0;
  string key = RGW_OBJ_TORRENT;
  rgw_obj obj(s->bucket, s->object.name);    

  rgw_raw_obj raw_obj;
  store->obj_to_raw(s->bucket_info.placement_rule, obj, &raw_obj);

  auto obj_ctx = store->svc.sysobj->init_obj_ctx();
  auto sysobj = obj_ctx.get_obj(raw_obj);

  op_ret = sysobj.omap().set(key, bl, null_yield);
  if (op_ret < 0)
  {
    ldout(s->cct, 0) << "ERROR: failed to omap_set() op_ret = " << op_ret << dendl;
    return op_ret;
  }

  return op_ret;
}
