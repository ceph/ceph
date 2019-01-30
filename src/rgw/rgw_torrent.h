// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_TORRENT_H
#define CEPH_RGW_TORRENT_H

#include <string>
#include <list>
#include <map>
#include <set>

#include "common/ceph_time.h"

#include "rgw_rados.h"
#include "rgw_common.h"

using ceph::crypto::SHA1;
using namespace std::literals;

struct req_state;

inline constexpr auto RGW_OBJ_TORRENT = "rgw.torrent"sv;
inline constexpr auto ANNOUNCE = "announce"sv;
inline constexpr auto ANNOUNCE_LIST = "announce-list"sv;
inline constexpr auto COMMENT = "comment"sv;
inline constexpr auto CREATED_BY = "created by"sv;
inline constexpr auto CREATION_DATE = "creation date"sv;
inline constexpr auto ENCODING = "encoding"sv;
inline constexpr auto LENGTH = "length"sv;
inline constexpr auto NAME = "name"sv;
inline constexpr auto PIECE_LENGTH = "piece length"sv;
inline constexpr auto PIECES = "pieces"sv;
inline constexpr auto INFO_PIECES = "info"sv;
inline constexpr auto GET_TORRENT = "torrent"sv;

class TorrentBencode
{
public:
  TorrentBencode() {}
  ~TorrentBencode() {}

  //control characters
  void bencode_dict(bufferlist& bl) { bl.append('d'); }
  void bencode_list(bufferlist& bl) { bl.append('l'); }
  void bencode_end(bufferlist& bl) { bl.append('e'); }

  //single values
  void bencode(int value, bufferlist& bl) 
  {
    bl.append('i');
    char info[100] = { 0 };
    sprintf(info, "%d", value);
    bl.append(info, strlen(info));
    bencode_end(bl);
  }

  //single values
  void bencode(std::string_view str, bufferlist& bl) 
  {
    bencode_key(str, bl);
  }

  //dictionary elements
  void bencode(std::string_view key, int value, bufferlist& bl) 
  {
    bencode_key(key, bl);
    bencode(value, bl);
  }

  //dictionary elements
  void bencode(std::string_view key, const std::string& value, bufferlist& bl) 
  {
    bencode_key(key, bl);
    bencode(value, bl);
  }

  //key len
  void bencode_key(std::string_view key, bufferlist& bl)
  {
    int len = key.length();
    char info[100] = { 0 };
    sprintf(info, "%d:", len);
    bl.append(info, strlen(info));
    bl.append(key.data(), len);
  }
};

/* torrent file struct */
class seed
{
private:
  struct
  {
    int piece_length;    // each piece length
    bufferlist sha1_bl;  // save sha1
    string name;    // file name
    off_t len;    // file total bytes
  }info;

  string  announce;    // tracker
  string origin; // origin
  time_t create_date{0};    // time of the file created
  string comment;  // comment
  string create_by;    // app name and version
  string encoding;    // if encode use gbk rather than gtf-8 use this field
  uint64_t sha_len;  // sha1 length
  bool is_torrent;  // flag
  bufferlist bl;  // bufflist ready to send

  struct req_state *s{nullptr};
  rgw::sal::RGWRadosStore *store{nullptr};
  SHA1 h;

  TorrentBencode dencode;
public:
  seed();
  ~seed();

  int get_params();
  void init(struct req_state *p_req, rgw::sal::RGWRadosStore *p_store);
  int get_torrent_file(RGWRados::Object::Read &read_op,
                       uint64_t &total_len,
                       ceph::bufferlist &bl_data,
                       rgw_obj &obj);
  
  off_t get_data_len();
  bool get_flag();

  void set_create_date(ceph::real_time& value);
  void set_info_name(const string& value);
  void update(bufferlist &bl);
  int complete();

private:
  void do_encode ();
  void set_announce();
  void set_exist(bool exist);
  void set_info_pieces(char *buff);
  void sha1(SHA1 *h, bufferlist &bl, off_t bl_len);
  int save_torrent_file();
};
#endif /* CEPH_RGW_TORRENT_H */
