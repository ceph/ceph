
#ifndef __OSD_H
#define __OSD_H

typedef size_t object_t;

// functions
int osd_read(int osd, object_t oid, size_t offset, size_t len, void *buf, Context *c);
int osd_read(int osd, object_t oid, size_t offset, size_t len, void **bufptr, size_t *buflen, Context *c);
int osd_write(int osd, object_t oid, size_t offset, size_t len, void *buf, Context *c);
int osd_remove(int osd, object_t oid, Context *c);


#endif
