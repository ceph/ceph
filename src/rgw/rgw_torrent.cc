#include <errno.h>
#include <stdlib.h>

#include <sstream>

#include "rgw_torrent.h"
#include "include/rados/librados.hpp"

#define dout_subsys ceph_subsys_rgw

using namespace std;
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
  torrent_bl.clear();
  s = NULL;
  store = NULL;
}

void seed::init(struct req_state *p_req, RGWRados *p_store)
{
  s = p_req;
  store = p_store;
}

void seed::get_torrent_file(int &op_ret, RGWRados::Object::Read &read_op, uint64_t &total_len, 
  bufferlist &bl_data, rgw_obj &obj)
{
  string oid, key;
  rgw_bucket bucket;
  map<string, bufferlist> m;
  set<string> obj_key;
  get_obj_bucket_and_oid_loc(obj, bucket, oid, key);
  ldout(s->cct, 0) << "NOTICE: head obj oid= " << oid << dendl;

  obj_key.insert(RGW_OBJ_TORRENT);
  op_ret = read_op.state.io_ctx.omap_get_vals_by_keys(oid, obj_key, &m);
  if (op_ret < 0)
  {
    ldout(s->cct, 0) << "ERROR: failed to omap_get_vals_by_keys op_ret = " << op_ret << dendl;
    return;
  }

  map<string, bufferlist>::iterator iter;
  for (iter = m.begin(); iter != m.end(); ++iter)
  {
    bufferlist bl_tmp = iter->second;
    char *pbuff = bl_tmp.c_str();
    bl.append(pbuff, bl_tmp.length());
  }

  bl_data = bl;
  total_len = bl.length();
  return;
}

bool seed::get_flag()
{
  return is_torrent;
}

void seed::save_data(bufferlist &bl)
{
  info.len += bl.length();
  torrent_bl.push_back(bl);
}

off_t seed::get_data_len()
{
  return info.len;
}

void seed::set_create_date(ceph::real_time value)
{
  utime_t date = ceph::real_clock::to_timespec(value);
  create_date = date.sec();
}

void seed::set_info_pieces(char *buff)
{
  info.sha1_bl.append(buff, CEPH_CRYPTO_SHA1_DIGESTSIZE);
}

void seed::set_info_name(string value)
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
    h->Update((byte *)pstr, info.piece_length);
    h->Final((byte *)sha);
    set_info_pieces(sha);
    pstr += info.piece_length;
  }

  /* process remain */
  if (0 != remain)
  {
    memset(sha, 0x00, sizeof(sha));
    h->Update((byte *)pstr, remain);
    h->Final((byte *)sha);
    set_info_pieces(sha);
  }
}

int seed::sha1_process()
{
  uint64_t remain = info.len%info.piece_length;
  uint8_t  remain_len = ((remain > 0)? 1 : 0);
  sha_len = (info.len/info.piece_length + remain_len)*CEPH_CRYPTO_SHA1_DIGESTSIZE;

  SHA1 h;
  list<bufferlist>::iterator iter = torrent_bl.begin();
  for (; iter != torrent_bl.end(); iter++)
  {
    bufferlist &bl_info = *iter;
    sha1(&h, bl_info, (*iter).length());
  }

  return 0;
}

int seed::handle_data()
{
  int ret = 0;

  /* sha1 process */
  ret = sha1_process();
  if (0 != ret)
  {
    ldout(s->cct, 0) << "ERROR: failed to sha1_process() ret= "<< ret << dendl;
    return ret;
  }

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

int seed::get_params()
{
  is_torrent = true;
  info.piece_length = g_conf->rgw_torrent_sha_unit;
  create_by = g_conf->rgw_torrent_createby;
  encoding = g_conf->rgw_torrent_encoding;
  origin = g_conf->rgw_torrent_origin;
  comment = g_conf->rgw_torrent_comment;
  announce = g_conf->rgw_torrent_tracker;

  /* tracker and tracker list is empty, set announce to origin */
  if (announce.empty() && !origin.empty())
  {
    announce = origin;
  }

  return 0;
}

void seed::set_announce()
{
  const char *pstr = announce.c_str();
  char *pstart = const_cast<char *>(pstr);
  uint64_t len = announce.length();
  char *pend = pstart + len;
  char *location;
  char flag = ',';

  list<string> announce_list;  // used to get announce list from conf
  while (pstart < pend)
  {
    location = NULL;
    location = strchr(pstart, flag);
    if (!location)
    {
      string ann(pstart);
      announce_list.push_back(ann);
      break;
    }

    char temp[100] = { 0 };
    snprintf(temp, location - pstart + 1, "%s", pstart);
    string anno(temp);
    announce_list.push_back(anno);
    pstart = location + 1;
  }

  list<string>::iterator iter = announce_list.begin();
  do_flush_bufflist(BE_STR, &(*iter), ANNOUNCE);
  iter++;

  do_flush_bufflist(BE_STR, NULL, ANNOUNCE_LIST);
  get_type(BE_LIST);
  for (; iter != announce_list.end(); ++iter)
  {
    get_type(BE_LIST);
    do_flush_bufflist(BE_STR, &(*iter), "");
    add_e();
  }
  add_e();
}

void seed::do_encode()
{
  get_type(BE_DICT);
  set_announce();

  if (!comment.empty())
  {
    do_flush_bufflist(BE_STR, &comment, COMMENT);
  }
  if (!create_by.empty())
  {
    do_flush_bufflist(BE_STR, &create_by, CREATED_BY);
  }
  do_flush_bufflist(BE_TIME_T, &create_date, CREATION_DATE);
  if (!encoding.empty())
  {
    do_flush_bufflist(BE_STR, &encoding, ENCODING);
  }
  
  do_flush_bufflist(BE_STR, NULL, INFO_PIECES);
  get_type(BE_DICT);
  do_flush_bufflist(BE_INT64, &info.len, LENGTH);
  do_flush_bufflist(BE_STR, &info.name, NAME);
  do_flush_bufflist(BE_INT64, &info.piece_length, PIECE_LENGTH);
  do_flush_bufflist(BE_STR, &info.sha1_bl, PIECES);
  add_e();

  add_e();
}

void seed::add_e()
{
  const char *p = "e";
  bl.append(p, 1);
}

void seed::get_type(be_type type)
{
  switch(type)
  {
    case BE_STR:
    {
      return;
    }
    case BE_INT:
    case BE_INT64:
    case BE_TIME_T:
    {
      const char *p = "i";
      bl.append(p, 1);
      break;
    }
    case BE_LIST:
    {
      const char *p = "l";
      bl.append(p, 1);
      break;
    }
    case BE_DICT:
    {
      const char *p = "d";
      bl.append(p, 1);
      break;
    }
  }
}

void seed::do_flush_bufflist(be_type type, void* src_data, const char *field)
{
  uint64_t totalLen = 0;
  uint64_t field_len_Len = 0;
  char cfield_len[100] = { 0 };
  int field_len = 0;
  field_len = strlen(field);

  if (0 != field_len)
  {
    totalLen += field_len;
    sprintf(cfield_len, "%d", field_len); 
    field_len_Len = strlen(cfield_len);
    totalLen += field_len_Len;
    char info[100] = { 0 };
    sprintf(info, "%d:%s", field_len, field);
    bl.append(info, totalLen + 1);
    get_type(type);
  }
  else
  {
    get_type(type);
  }

  if (NULL == src_data)
  {
    return;
  }

  switch (type)
  {
    case BE_STR:
    {
      flush_bufflist(type, src_data, field);
      break;
    }

    case BE_INT:
    case BE_INT64:
    case BE_TIME_T:
    {
      flush_bufflist(type, src_data);
      break;
    }

    default:
    {
      break;
    }
  }
}

void seed::flush_bufflist(be_type type, void *src_data, const char *field)
{
  char infolen[100] = { 0 };
  switch (type)
  {
    case BE_STR:
    {
      /* set pieces value */
      if (0 == strcmp(PIECES, field))
      {
        sprintf(infolen, "%ld:", sha_len); 
        bl.append(infolen, strlen(infolen));
        bl.append(info.sha1_bl.c_str(), sha_len);
        break;
      }

      const char *pstr = (static_cast<string *>(src_data))->c_str();
      int len = strlen(pstr);
      sprintf(infolen, "%d:", len); 
      bl.append(infolen, strlen(infolen));
      bl.append(pstr, len);
      break;
    }
    case BE_INT:
    case BE_INT64:
    case BE_TIME_T:
    {
      if (type == BE_INT)
      {
        int *p = static_cast<int *>(src_data);
        sprintf(infolen, "%d", *p); 
      }
      else if (type == BE_INT64)
      {
        uint64_t *p = static_cast<uint64_t *>(src_data);
        sprintf(infolen, "%ld", *p); 
      }
      else
      {
        long *p = static_cast<long *>(src_data);
        sprintf(infolen, "%ld", *p); 
      }
      bl.append(infolen, strlen(infolen));
      add_e();
      break;
    }
    default:
    {
      break;
    }
  }
}

int seed::save_torrent_file()
{
  int op_ret = 0;
  string key = RGW_OBJ_TORRENT;
  rgw_obj obj(s->bucket, s->object.name);    

  op_ret = store->omap_set(obj, key, bl);
  if (op_ret < 0)
  {
    ldout(s->cct, 0) << "ERROR: failed to omap_set() op_ret = " << op_ret << dendl;
    return op_ret;
  }

  return op_ret;
}
