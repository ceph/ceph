#ifndef __S3ACCESS_H
#define __S3ACCESS_H

#ifdef __cplusplus
extern "C" {
#endif

typedef void *S3AccessHandle;

int list_buckets_init(const char *id, S3AccessHandle *handle);
int list_buckets_next(const char *id, char *buf, int size, S3AccessHandle *handle);

#ifdef __cplusplus
}
#endif

#endif
