#include <errno.h>

#include "rgw_common.h"
#include "rgw_acl.h"

/* Loglevel of the gateway */
int rgw_log_level = 20;

int parse_time(const char *time_str, time_t *time)
{
  struct tm tm;
  memset(&tm, 0, sizeof(struct tm));
  if (!strptime(time_str, "%a, %d %b %Y %H:%M:%S %Z", &tm))
    return -EINVAL;

  *time = mktime(&tm);

  return 0;
}

int NameVal::parse()
{
  int delim_pos = str.find('=');
  int ret = 0;

  if (delim_pos < 0) {
    name = str;
    val = "";
    ret = 1;
  } else {
    name = str.substr(0, delim_pos);
    val = str.substr(delim_pos + 1);
  }

  RGW_LOG(10) << "parsed: name=" << name << " val=" << val << endl;
  return ret; 
}

int XMLArgs::parse()
{
  int pos = 0, fpos;
  bool end = false;
  if (str[pos] == '?') pos++;

  while (!end) {
    fpos = str.find('&', pos);
    if (fpos  < pos) {
       end = true;
       fpos = str.size(); 
    }
    NameVal nv(str.substr(pos, fpos - pos));
    int ret = nv.parse();
    if (ret >= 0) {
      val_map[nv.get_name()] = nv.get_val();

      if (ret > 0) { /* this might be a sub-resource */
        if ((nv.get_name().compare("acl") == 0) ||
            (nv.get_name().compare("location") == 0) ||
            (nv.get_name().compare("torrent") == 0))
          sub_resource = nv.get_name();
      }
    }

    pos = fpos + 1;  
  }

  return 0;
}

string& XMLArgs::get(string& name)
{
  map<string, string>::iterator iter;
  iter = val_map.find(name);
  if (iter == val_map.end())
    return empty_str;
  return iter->second;
}

string& XMLArgs::get(const char *name)
{
  string s(name);
  return get(s);
}

bool verify_permission(RGWAccessControlPolicy *policy, string& uid, int perm)
{
   if (!policy)
     return false;

   int acl_perm = policy->get_perm(uid, perm);

   return (perm == acl_perm);
}

bool verify_permission(struct req_state *s, int perm)
{
  return verify_permission(s->acl, s->user.user_id, perm);
}

static char hex_to_num(char c)
{
  static char table[256];
  static bool initialized = false;


  if (!initialized) {
    memset(table, -1, sizeof(table));
    int i;
    for (i = '0'; i<='9'; i++)
      table[i] = i - '0';
    for (i = 'A'; i<='F'; i++)
      table[i] = i - 'A' + 0xa;
    for (i = 'a'; i<='f'; i++)
      table[i] = i - 'a' + 0xa;
  }
  return table[(int)c];
}

bool url_decode(string& src_str, string& dest_str)
{
  RGW_LOG(10) << "in url_decode with " << src_str << endl;
  const char *src = src_str.c_str();
  char dest[src_str.size()];
  int pos = 0;
  char c;

  RGW_LOG(10) << "src=" << (void *)src << std::endl;

  while (*src) {
    if (*src != '%') {
      if (*src != '+') {
	dest[pos++] = *src++;
      } else {
	dest[pos++] = ' ';
	++src;
      }
    } else {
      src++;
      char c1 = hex_to_num(*src++);
      c = c1 << 4;
      if (c1 < 0)
        return false;
      c1 = hex_to_num(*src++);
      if (c1 < 0)
        return false;
      c |= c1;
      dest[pos++] = c;
    }
  }
  dest[pos] = 0;
  dest_str = dest;

  return true;
}


