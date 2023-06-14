// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_torrent.h"
#include <ctime>
#include <fmt/format.h>
#include "common/split.h"
#include "rgw_sal.h"

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

//control characters
void bencode_dict(bufferlist& bl) { bl.append('d'); }
void bencode_list(bufferlist& bl) { bl.append('l'); }
void bencode_end(bufferlist& bl) { bl.append('e'); }

//key len
void bencode_key(std::string_view key, bufferlist& bl)
{
  bl.append(fmt::format("{}:", key.size()));
  bl.append(key);
}

//single values
void bencode(int value, bufferlist& bl)
{
  bl.append(fmt::format("i{}", value));
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

  bencode_dict(bl);

  auto trackers = ceph::split(conf->rgw_torrent_tracker, ",");
  if (auto i = trackers.begin(); i != trackers.end()) {
    bencode_key(ANNOUNCE, bl);
    bencode_key(*i, bl);

    bencode_key(ANNOUNCE_LIST, bl);
    bencode_list(bl);
    for (; i != trackers.end(); ++i) {
      bencode_list(bl);
      bencode_key(*i, bl);
      bencode_end(bl);
    }
    bencode_end(bl);
  }

  std::string_view comment = conf->rgw_torrent_comment;
  if (!comment.empty()) {
    bencode(COMMENT, comment, bl);
  }
  std::string_view create_by = conf->rgw_torrent_createby;
  if (!create_by.empty()) {
    bencode(CREATED_BY, create_by, bl);
  }
  std::string_view encoding = conf->rgw_torrent_encoding;
  if (!encoding.empty()) {
    bencode(ENCODING, encoding, bl);
  }

  // append the info stored in the object
  bl.append(std::move(infobl));
  return 0;
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
      char out[ceph::crypto::SHA1::digest_size];
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
      char out[ceph::crypto::SHA1::digest_size];
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

  // Only encode create_date and sha1 info. Other fields will be added during
  // GetObjectTorrent by rgw_read_torrent_file()
  // issue tracked here: https://tracker.ceph.com/issues/61160
  // coverity[store_truncates_time_t:SUPPRESS]
  bencode(CREATION_DATE, std::time(nullptr), bl);

  bencode_key(INFO_PIECES, bl);
  bencode_dict(bl);
  bencode(LENGTH, len, bl);
  bencode(NAME, filename, bl);
  bencode(PIECE_LENGTH, piece_len, bl);

  bencode_key(PIECES, bl);
  bl.append(std::to_string(piece_count));
  bl.append(':');
  bl.append(piece_hashes);
  bencode_end(bl);

  return bl;
}
