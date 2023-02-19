// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <string>
#include <list>
#include <map>
#include <set>

#include <fmt/format.h>

#include "common/ceph_time.h"

#include "rgw_common.h"

struct req_state;

#define RGW_OBJ_TORRENT    "rgw.torrent"

#define ANNOUNCE           "announce"
#define ANNOUNCE_LIST      "announce-list"
#define COMMENT            "comment"
#define CREATED_BY         "created by"
#define CREATION_DATE      "creation date"
#define ENCODING           "encoding"
#define LENGTH             "length"
#define NAME               "name"
#define PIECE_LENGTH       "piece length"
#define PIECES             "pieces"
#define INFO_PIECES        "info"
#define GET_TORRENT        "torrent"

class TorrentBencode
{
public:
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
  void bencode(std::string_view key, std::string_view value, bufferlist& bl)
  {
    bencode_key(key, bl);
    bencode(value, bl);
  }

  //key len
  void bencode_key(std::string_view key, bufferlist& bl)
  {
    bl.append(fmt::format("{}:", key.size()));
    bl.append(key);
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
    std::string name;    // file name
    off_t len;    // file total bytes
  }info;

  std::string  announce;    // tracker
  std::string origin; // origin
  time_t create_date{0};    // time of the file created
  std::string comment;  // comment
  std::string create_by;    // app name and version
  std::string encoding;    // if encode use gbk rather than gtf-8 use this field
  uint64_t sha_len;  // sha1 length
  bool is_torrent;  // flag
  bufferlist bl;  // bufflist ready to send

  req_state *s{nullptr};
  rgw::sal::Driver* driver{nullptr};
  ceph::crypto::SHA1 h;

  TorrentBencode dencode;
public:
  seed();
  ~seed();

  int get_params();
  void init(req_state *p_req, rgw::sal::Driver* _driver);
  int get_torrent_file(rgw::sal::Object* object,
                       uint64_t &total_len,
                       ceph::bufferlist &bl_data,
                       rgw_obj &obj);

  off_t get_data_len();
  bool get_flag();

  void set_create_date(ceph::real_time& value);
  void set_info_name(const std::string& value);
  void update(bufferlist &bl);
  int complete(optional_yield y);

private:
  void do_encode ();
  void set_announce();
  void set_exist(bool exist);
  void set_info_pieces(char *buff);
  void sha1(ceph::crypto::SHA1 *h, bufferlist &bl, off_t bl_len);
  int save_torrent_file(optional_yield y);
};
