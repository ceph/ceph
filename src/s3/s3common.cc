#include <errno.h>

#include "s3common.h"

int parse_time(const char *time_str, time_t *time)
{
  struct tm tm;
  memset(&tm, 0, sizeof(struct tm));
  if (!strptime(time_str, "%FT%T%z", &tm))
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

  cout << "parsed: name=" << name << " val=" << val << std::endl;
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


