
#include <iostream>
#include <errno.h>

#include "include/types.h"
#include "objclass/objclass.h"

#include "cls_trivialmap.h"

CLS_VER(1,0)
CLS_NAME(trivialmap)

cls_handle_t h_class;
cls_method_handle_t h_read_all;
cls_method_handle_t h_update;

static int read_all(cls_method_context_t ctx, bufferlist *inbl, bufferlist *outbl)
{
  return cls_cxx_read(ctx, 0, 0, outbl);
}

static int update(cls_method_context_t ctx, bufferlist *inbl, bufferlist *outbl)
{
  bufferlist::iterator ip = inbl->begin();

  // read the whole object
  bufferlist bl;
  int r = cls_cxx_read(ctx, 0, 0, &bl);
  if (r < 0)
    return r;

  // parse
  bufferlist header;
  map<nstring, bufferlist> m;
  if (bl.length()) {
    bufferlist::iterator p = bl.begin();
    ::decode(header, p);
    ::decode(m, p);
    assert(p.end());
  }
 
  // do the update(s)
  while (!ip.end()) {
    __u8 op;
    nstring key;
    ::decode(op, ip);
    ::decode(key, ip);
    
    switch (op) {
    case CLS_TRIVIALMAP_SET: // insert key
      {
	bufferlist data;
	::decode(data, ip);
	m[key] = data;
      }
      break;

    case CLS_TRIVIALMAP_RM: // remove key
      m.erase(key);
      break;

    case CLS_TRIVIALMAP_HDR: // update header
      {
	::decode(header, ip);
      }
      break;

    default:
      return -EINVAL;
    }
  }

  // reencode
  bufferlist obl;
  ::encode(header, obl);
  ::encode(m, obl);

  // write it out
  cls_cxx_replace(ctx, 0, obl.length(), &obl);

  return 0;
}


void class_init()
{
   cls_register("trivialmap", &h_class);
   cls_register_cxx_method(h_class, "read_all", CLS_METHOD_RD, read_all, &h_read_all);
   cls_register_cxx_method(h_class, "update", CLS_METHOD_WR, update, &h_update);
   return;
}

