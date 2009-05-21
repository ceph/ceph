
#include "common/ClassVersion.h"
#include "include/ClassLibrary.h"

#include "config.h"

ClassInfo *ClassVersionMap::get(ClassVersion& ver)
{
  ClassVersion v = ver;
  tClassVersionMap::iterator iter;

  if (ver.is_default()) {
    v.ver = default_ver;
  }

  iter = m.find(ver);

  if (iter != m.end())
    return &(iter->second);

  return NULL;
}

