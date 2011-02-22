
#include "common/ClassVersion.h"
#include "include/ClassLibrary.h"

#include "common/config.h"

ClassInfo *ClassVersionMap::get(ClassVersion& ver)
{
  ClassVersion v = ver;
  tClassVersionMap::iterator iter;

  if (ver.is_default()) {
    v.ver = default_ver;
  }
  dout(0) << "ClassVersionMap getting version " << v << " (requested " << ver << ")" << dendl;

  iter = m.find(v);

  if (iter != m.end())
    return &(iter->second);

  return NULL;
}

