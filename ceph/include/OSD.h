
#ifndef __OSD_H
#define __OSD_H

#include "Context.h"

typedef size_t object_t;

// functions
int osd_read(int osd, object_t oid, size_t len, size_t offset, void *buf, Context *c);
int osd_read_all(int osd, object_t oid, void **bufptr, size_t *buflen, Context *c);

int osd_write(int osd, object_t oid, size_t len, size_t offset, void *buf, int flags, Context *c);

int osd_remove(int osd, object_t oid, Context *c);


#endif
