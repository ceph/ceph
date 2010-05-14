


#include <iostream>
#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include <openssl/md5.h>
#include <openssl/sha.h>

#include "include/types.h"
#include "objclass/objclass.h"

#include "include/rbd_types.h"

CLS_VER(1,0)
CLS_NAME(rbd)

cls_handle_t h_class;
cls_method_handle_t h_snapshots_list;

int snapshots_list(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  int snap_count = 0;
  uint64_t snap_names_len = 0;
  int rc;
  struct rbd_obj_header_ondisk *header;
  bufferlist bl;

  cls_log("snapshots_list");

  while (1) {
    int len = sizeof(*header) +
      snap_count * sizeof(struct rbd_obj_snap_ondisk) +
      snap_names_len;

    rc = cls_cxx_read(hctx, 0, len, &bl);
    if (rc < 0)
      return rc;

    header = (struct rbd_obj_header_ondisk *)bl.c_str();

    if ((snap_count != header->snap_count) ||
        (snap_names_len != header->snap_names_len)) {
      snap_count = header->snap_count;
      snap_names_len = header->snap_names_len;
      bl.clear();
      continue;
    }
    break;
  }

  bufferptr p(snap_names_len);
  memcpy(p.c_str(),
         bl.c_str() + sizeof(*header) + snap_count * sizeof(struct rbd_obj_snap_ondisk),
         snap_names_len);

  out->push_back(p);

  return snap_count;
}

void class_init()
{
   cls_log("Loaded rbd class!");

   cls_register("rbd", &h_class);
   cls_register_cxx_method(h_class, "snap_list", CLS_METHOD_RD, snapshots_list, &h_snapshots_list);

   return;
}

