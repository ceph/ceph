#ifndef __S3ACCESS_H
#define __S3ACCESS_H

typedef void *S3AccessHandle;

int list_buckets_init(const char *id, S3AccessHandle *handle);
int list_buckets_next(const char *id, char *buf, int size, S3AccessHandle *handle);

#endif
