#ifndef CEPH_RGW_MONGOOSE_H
#define CEPH_RGW_MONGOOSE_H

#include "rgw_client_io.h"


struct mg_event;
struct mg_connection;


class RGWMongoose : public RGWClientIO
{
  mg_event *event;

  bufferlist header_data;
  bufferlist data;

  bool header_done;
  bool sent_header;
  bool has_content_length;

public:
  void init_env(CephContext *cct);

  int write_data(const char *buf, int len);
  int read_data(char *buf, int len);

  int send_status(const char *status, const char *status_name);
  int send_100_continue();
  int complete_header();
  int complete_request();
  int send_content_length(uint64_t len);

  RGWMongoose(mg_event *_event);
  void flush();
};


#endif
