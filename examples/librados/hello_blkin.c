// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <stdio.h>
#include <rados/librados.h>
#include <stdlib.h>
#include <blkin-front.h>

#define POOL "data"

const char *object_name = "hello_world_object";
const char *object_val = "hello world!";

rados_ioctx_t *connect_to_rados()
{
  rados_t rados;
  rados_ioctx_t *ioctx = malloc(sizeof(rados_ioctx_t));

  if (rados_create(&rados, NULL) < 0) {
    return NULL;
  }
  if (rados_conf_read_file(rados, NULL) < 0){
    return NULL;
  }
  if (rados_connect(rados) < 0) {
    printf("Could not connect\n");
    rados_shutdown(rados);
    return NULL;
  }
  if (rados_pool_lookup(rados, POOL) < 0) {
    printf("Could not find pool\n");
    rados_shutdown(rados);
    return NULL;
  }
  if (rados_ioctx_create(rados, POOL, ioctx) < 0){
    rados_shutdown(rados);
    return NULL;
  }
  return ioctx;
}

void end_read(rados_completion_t c, void *arg)
{
  fprintf(stderr, "just read\n");
}

void end_write(rados_completion_t c, void *arg)
{
  fprintf(stderr, "just wrote\n");
}

int main()
{
  int r, i;
  char temp[12];
  rados_completion_t compl;
  rados_ioctx_t *rados_ctx;
  /*initialize lib*/
  r = blkin_init();
  if (r < 0) {
    fprintf(stderr, "Could not initialize blkin\n");
    exit(1);
  }
  /*initialize endpoint*/
  struct blkin_endpoint endp;
  blkin_init_endpoint(&endp, "10.0.0.1", 5000, "rados client");
  struct blkin_trace trace;
  blkin_init_new_trace(&trace, "client", &endp);
  BLKIN_TIMESTAMP(&trace, &endp, "start");

  //connect
  rados_ctx = connect_to_rados();
  if (rados_ctx == NULL){
    printf("Connect return NULL\n");
    exit(1);
  }
  printf("connected\n");

  //write an object
  r = rados_aio_create_completion(NULL, end_write, end_write, &compl);
  if (r<0) {
    printf("could not create completion\n");
    exit(1);
  }
  printf("created completion\n");
  r = rados_aio_write_traced(*rados_ctx, object_name, compl, object_val, 12,
      0, &trace.info);

  rados_aio_wait_for_safe(compl);
  rados_aio_release(compl);
  printf("completed\n");

  //read an object
  r = rados_aio_create_completion(NULL, end_read, end_read, &compl);
  if (r<0) {
    printf("could not create completion\n");
    exit(1);
  }
  printf("created completion\n");
  r = rados_aio_read_traced(*rados_ctx, object_name, compl, temp, 12, 0,
      &trace.info);
  rados_aio_wait_for_safe(compl);
  rados_aio_release(compl);
  printf("I read:\n");
  for (i=0;i<12;i++)
    printf("%c",temp[i]);
  printf("completed\n");
  BLKIN_TIMESTAMP(&trace, &endp, "Span ended");
}
