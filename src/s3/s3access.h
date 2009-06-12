#ifndef __S3ACCESS_H
#define __S3ACCESS_H

#include <string>
#include <vector>

typedef void *S3AccessHandle;

int list_buckets_init(std::string& id, S3AccessHandle *handle);
int list_buckets_next(std::string& id, char *buf, int size, S3AccessHandle *handle);

int list_objects(std::string& id, std::string& bucket, int max, std::string& prefix, std::string& marker, std::vector<std::string>& result);

#endif
