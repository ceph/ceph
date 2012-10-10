#ifndef CEPH_RGW_STRING_H
#define CEPH_RGW_STRING_H

struct ltstr_nocase
{
  bool operator()(const string& s1, const string& s2) const
  {
    return strcasecmp(s1.c_str(), s2.c_str()) < 0;
  }
};

static inline int stringcasecmp(const string& s1, const string& s2)
{
  return strcasecmp(s1.c_str(), s2.c_str());
}

static inline int stringcasecmp(const string& s1, const char *s2)
{
  return strcasecmp(s1.c_str(), s2);
}

static inline int stringcasecmp(const string& s1, int ofs, int size, const string& s2)
{
  return strncasecmp(s1.c_str() + ofs, s2.c_str(), size);
}

#endif
