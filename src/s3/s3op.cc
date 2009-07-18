
#include <errno.h>

#include "s3access.h"
#include "s3op.h"
#include "s3rest.h"

static int parse_range(const char *range, off_t ofs, off_t end)
{
  int r = -ERANGE;
  string s(range);
  int pos = s.find("bytes=");
  string ofs_str;
  string end_str;

  if (pos < 0)
    goto done;

  s = s.substr(pos + 6); /* size of("bytes=")  */
  pos = s.find('-');
  if (pos < 0)
    goto done;

  ofs_str = s.substr(0, pos);
  end_str = s.substr(pos + 1);
  ofs = atoll(ofs_str.c_str());
  end = atoll(end_str.c_str());

  if (end < ofs)
    goto done;

  r = 0;
done:
  return r;
}

void S3GetObj::execute()
{
  ret = get_params();
  if (ret < 0)
    goto done;

  len = s3store->get_obj(s->bucket_str, s->object_str, &data, ofs, end, &attrs,
                         mod_ptr, unmod_ptr, if_match, if_nomatch, get_data, &err);
  if (len < 0)
    ret = len;

done:
  send_response();
}

int S3GetObj::init()
{
  if (range_str) {
    int r = parse_range(range_str, ofs, end);
    if (r < 0)
      return r;
  }
  if (if_mod) {
    if (parse_time(if_mod, &mod_time) < 0)
      return -EINVAL;
    mod_ptr = &mod_time;
  }

  if (if_unmod) {
    if (parse_time(if_unmod, &unmod_time) < 0)
      return -EINVAL;
    unmod_ptr = &unmod_time;
  }

  return 0;
}

void S3ListBucket::execute()
{
  prefix = s->args.get("prefix");
  marker = s->args.get("marker");
  max_keys = s->args.get("max-keys");
 if (!max_keys.empty()) {
    max = atoi(max_keys.c_str());
  } else {
    max = -1;
  }
  delimiter = s->args.get("delimiter");
  ret = s3store->list_objects(s->user.user_id, s->bucket_str, max, prefix, delimiter, marker, objs, common_prefixes);

  send_response();
}


