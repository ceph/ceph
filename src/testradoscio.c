// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "include/librados.h"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

int main(int argc, const char **argv) {
  if (argc<2) {
    printf("need to assign # of concurrent ios to perform!\n");
    exit(1);
  }
	
  int concurrentios = atoi(argv[1]);
  printf("Performing %d ios\n", concurrentios);

  if (rados_initialize(argc, argv) < 0) {
    printf("error initializing\n");
    exit(1);
  }
	
  rados_pool_t pool;
  int r = rados_open_pool("data", &pool);
  printf("open pool result = %d, pool = %d\n", r, pool);

  rados_completion_t completions[concurrentios];
  char* name[concurrentios];
  char contents[128];
  //	char* buffers[concurrentios];
  int i;
  for (i=0; i<concurrentios; i +=1 ) {
    printf("on loop iteration %d\n", i);
    name[i] = (char*) malloc(sizeof(char)*128);
    snprintf(name[i], 127, "%d", i);
    snprintf(contents, 127, "I'm the %d object!", i);
    rados_aio_write(pool, name[i], 0, &contents, 127, &completions[i]);
    printf("asynchronously writing \"%s\"\n", contents);
  }
  printf("Completed asynchronous writes to objects\n");
	
  rados_completion_t read_completions[concurrentios];
  char* readData[concurrentios];
  for (i=0; i<concurrentios; i+=1 ) {
    printf("Waiting for %d write to be complete\n", i);
    rados_aio_wait_for_safe(completions[i]);
    printf("Write %d is complete!\n", i);
    readData[i] = (char*) malloc(sizeof(char)*128);
    rados_aio_read(pool, name[i], 0, readData[i], 127, &read_completions[i]);
    //    rados_aio_release(completions[i]);
  }
  for (i=0; i<concurrentios; i+=1) {
    printf("Waiting for %d read\n", i);
    rados_aio_wait_for_complete(read_completions[i]);
    printf("Read %d is complete!\n", i);
    printf(readData[i]);
    // rados_aio_release(read_completions[i]);
  }
	
  for (i=0; i<concurrentios; i+=1) {
    free (name[i]);
    free (readData[i]);
  }

  rados_close_pool(pool);

  rados_deinitialize();

  return 0;
}

