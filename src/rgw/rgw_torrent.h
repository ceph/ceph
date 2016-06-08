#ifndef CEPH_RGW_TORRENT_H
#define CEPH_RGW_TORRENT_H

#include <string>
#include <list>
#include <map>
#include <set>

#include "common/ceph_time.h"

#include "rgw_rados.h"
#include "rgw_common.h"

using namespace std;
using ceph::crypto::SHA1;

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
#define GET_TORRENT        "get_torrent"

typedef enum
{
  BE_STR,
  BE_INT,
  BE_INT64,
  BE_TIME_T,
  BE_LIST,
  BE_DICT,
} be_type;

/* torrent file struct */
class seed
{
private:
  struct
  {
    long piece_length;    // each piece length
    bufferlist sha1_bl;  // save sha1
    string name;    // file name
    off_t len;    // file total bytes
  }info;

  string  announce;    // tracker
  string origin; // origin
  time_t create_date;    // time of the file created
  string comment;  // comment
  string create_by;    // app name and version
  string encoding;    // if encode use gbk rather than gtf-8 use this field
  uint64_t sha_len;  // sha1 length
  bool is_torrent;  // flag
  bufferlist bl;  // bufflist ready to send
  list<bufferlist> torrent_bl;   // meate data

  struct req_state *s;
  RGWRados *store;

public:
  seed();
  ~seed();

  int get_params();
  void init(struct req_state *p_req, RGWRados *p_store);
  void get_torrent_file(int &op_ret, RGWRados::Object::Read &read_op, 
    uint64_t &total_len, bufferlist &bl_data, rgw_obj &obj);
  
  off_t get_data_len();
  bool get_flag();

  int handle_data();
  void save_data(bufferlist &bl);
  void set_create_date(ceph::real_time value);
  void set_info_name(string value); 

private:
  void add_e();
  void do_encode ();
  void set_announce();
  void set_exist(bool exist);
  void get_type(be_type type);
  void set_info_pieces(char *buff);
  int sha1_process();
  void sha1(SHA1 *h, bufferlist &bl, off_t bl_len);
  void flush_bufflist(be_type type, void *src_data, const char *field = NULL);
  void do_flush_bufflist(be_type type, void* src_data, const char *field);
  int save_torrent_file();
};
#endif /* CEPH_RGW_TORRENT_H */
