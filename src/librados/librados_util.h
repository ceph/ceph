#include <cstdint>
#include "acconfig.h"
#include "include/rados/librados.h"
#include "IoCtxImpl.h"

#ifdef WITH_LTTNG
#include "tracing/librados.h"
#else
#define tracepoint(...)
#endif

uint8_t get_checksum_op_type(rados_checksum_type_t type);
int get_op_flags(int flags);
int translate_flags(int flags);

struct librados::ObjListCtx {
  librados::IoCtxImpl dupctx;
  librados::IoCtxImpl *ctx;
  Objecter::NListContext *nlc;
  bool legacy_list_api;

  ObjListCtx(IoCtxImpl *c, Objecter::NListContext *nl, bool legacy=false)
    : nlc(nl),
      legacy_list_api(legacy) {
    // Get our own private IoCtxImpl so that namespace setting isn't
    // changed by caller between uses.
    ctx = &dupctx;
    dupctx.dup(*c);
  }
  ~ObjListCtx() {
    ctx = NULL;
    delete nlc;
  }
};
