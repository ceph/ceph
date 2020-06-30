// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_GGATE_GGATE_DRV_H
#define CEPH_RBD_GGATE_GGATE_DRV_H

#ifdef __cplusplus
extern "C" {
#endif

#include <sys/param.h>

#include <stdbool.h>
#include <stdint.h>

typedef void *ggate_drv_t;
typedef void *ggate_drv_req_t;

/*
 * GGATE driver commands. They are mapped to GgateReq::Command.
 */
enum {
  GGATE_DRV_CMD_UNKNOWN = 0,
  GGATE_DRV_CMD_WRITE = 1,
  GGATE_DRV_CMD_READ = 2,
  GGATE_DRV_CMD_FLUSH = 3,
  GGATE_DRV_CMD_DISCARD = 4,
};

struct ggate_drv_info {
  char id[16];
  char name[NAME_MAX];
  char info[2048]; /* G_GATE_INFOSIZE */
};

uint64_t ggate_drv_req_id(ggate_drv_req_t req);
int ggate_drv_req_cmd(ggate_drv_req_t req);
void *ggate_drv_req_buf(ggate_drv_req_t req);
size_t ggate_drv_req_length(ggate_drv_req_t req);
uint64_t ggate_drv_req_offset(ggate_drv_req_t req);
int ggate_drv_req_error(ggate_drv_req_t req);

void ggate_drv_req_set_error(ggate_drv_req_t req, int error);
void *ggate_drv_req_release_buf(ggate_drv_req_t req);

int ggate_drv_load();

int ggate_drv_create(char *name, size_t namelen, size_t sectorsize,
    size_t mediasize, bool readonly, const char *info, ggate_drv_t *drv);
void ggate_drv_destroy(ggate_drv_t drv);

int ggate_drv_recv(ggate_drv_t drv, ggate_drv_req_t *req);
int ggate_drv_send(ggate_drv_t drv, ggate_drv_req_t req);

int ggate_drv_resize(ggate_drv_t drv, size_t newsize);

int ggate_drv_kill(const char *devname);
int ggate_drv_list(struct ggate_drv_info *info, size_t *size);

#ifdef __cplusplus
}
#endif

#endif // CEPH_RBD_GGATE_GGATE_DRV_H
