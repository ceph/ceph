#ifndef __C3_H
#define __C3_H

#ifdef __cplusplus
extern "C" {
#endif



/* initialization */
int c3_initialize(int argc, const char **argv); /* arguments are optional */
void c3_deinitialize();


/* read/write objects */
int c3_write(object_t *oid, const char *buf, off_t off, size_t len);
int c3_read(object_t *oid, char *buf, off_t off, size_t len);

#ifdef __cplusplus
}
#endif

#endif
