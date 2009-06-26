#ifndef __USER_H
#define __USER_H

#include <string>

#include "include/types.h"

using namespace std;

#define USER_INFO_BUCKET_NAME ".users"

#define USER_INFO_VER 1

struct S3UserInfo
{
  string user_id;
  string secret_key;
  string display_name;

  void encode(bufferlist& bl) const {
     __u32 ver = USER_INFO_VER;
     ::encode(ver, bl);
     ::encode(user_id, bl);
     ::encode(secret_key, bl);
     ::encode(display_name, bl);
  }
  void decode(bufferlist::iterator& bl) {
     __u32 ver;
    ::decode(ver, bl);
    ::decode(user_id, bl);
    ::decode(secret_key, bl);
    ::decode(display_name, bl);
  }
};
WRITE_CLASS_ENCODER(S3UserInfo)

extern int s3_get_user_info(string user_id, S3UserInfo& info);
extern int s3_store_user_info(S3UserInfo& info);

#endif
