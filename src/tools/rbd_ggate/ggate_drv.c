// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/param.h>
#include <sys/bio.h>
#include <sys/disk.h>
#include <sys/linker.h>
#include <sys/queue.h>
#include <sys/stat.h>

#include <geom/gate/g_gate.h>

#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <libgeom.h>

#include "debug.h"
#include "ggate_drv.h"

uint64_t ggate_drv_req_id(ggate_drv_req_t req) {
  struct g_gate_ctl_io *ggio = (struct g_gate_ctl_io *)req;

  return ggio->gctl_seq;
}

int ggate_drv_req_cmd(ggate_drv_req_t req) {
  struct g_gate_ctl_io *ggio = (struct g_gate_ctl_io *)req;

  switch (ggio->gctl_cmd) {
  case BIO_WRITE:
    return GGATE_DRV_CMD_WRITE;
  case BIO_READ:
    return GGATE_DRV_CMD_READ;
  case BIO_FLUSH:
    return GGATE_DRV_CMD_FLUSH;
  case BIO_DELETE:
    return GGATE_DRV_CMD_DISCARD;
  default:
    return GGATE_DRV_CMD_UNKNOWN;
  }
}

uint64_t ggate_drv_req_offset(ggate_drv_req_t req) {
  struct g_gate_ctl_io *ggio = (struct g_gate_ctl_io *)req;

  return ggio->gctl_offset;
}

size_t ggate_drv_req_length(ggate_drv_req_t req) {
  struct g_gate_ctl_io *ggio = (struct g_gate_ctl_io *)req;

  return ggio->gctl_length;
}

void *ggate_drv_req_buf(ggate_drv_req_t req) {
  struct g_gate_ctl_io *ggio = (struct g_gate_ctl_io *)req;

  return ggio->gctl_data;
}

int ggate_drv_req_error(ggate_drv_req_t req) {
  struct g_gate_ctl_io *ggio = (struct g_gate_ctl_io *)req;

  return ggio->gctl_error;
}

void ggate_drv_req_set_error(ggate_drv_req_t req, int error) {
  struct g_gate_ctl_io *ggio = (struct g_gate_ctl_io *)req;

  ggio->gctl_error = error;
}

void *ggate_drv_req_release_buf(ggate_drv_req_t req) {
  struct g_gate_ctl_io *ggio = (struct g_gate_ctl_io *)req;

  void *data = ggio->gctl_data;
  ggio->gctl_data = NULL;

  return data;
}

struct ggate_drv {
  int fd;
  int unit;
};

int ggate_drv_load() {
  if (modfind("g_gate") != -1) {
    /* Present in kernel. */
    return 0;
  }

  if (kldload("geom_gate") == -1 || modfind("g_gate") == -1) {
    if (errno != EEXIST) {
      err("failed to load geom_gate module");
      return -errno;
    }
  }
  return 0;
}

int ggate_drv_create(char *name, size_t namelen, size_t sectorsize,
    size_t mediasize, bool readonly, const char *info, ggate_drv_t *drv_) {
  struct ggate_drv *drv;
  struct g_gate_ctl_create ggiocreate;

  debug(20, "%s: name=%s, sectorsize=%zd, mediasize=%zd, readonly=%d, info=%s",
      __func__, name, sectorsize, mediasize, (int)readonly, info);

  if (*name != '\0') {
    if (namelen > sizeof(ggiocreate.gctl_name) - 1) {
      return -ENAMETOOLONG;
    }
  }

  /*
   * We communicate with ggate via /dev/ggctl. Open it.
   */
  int fd = open("/dev/" G_GATE_CTL_NAME, O_RDWR);
  if (fd == -1) {
    err("failed to open /dev/" G_GATE_CTL_NAME);
    return -errno;
  }

  drv = calloc(1, sizeof(*drv));
  if (drv == NULL) {
    errno = ENOMEM;
    goto fail_close;
  }

  /*
   * Create provider.
   */
  memset(&ggiocreate, 0, sizeof(ggiocreate));
  ggiocreate.gctl_version = G_GATE_VERSION;
  ggiocreate.gctl_mediasize = mediasize;
  ggiocreate.gctl_sectorsize = sectorsize;
  ggiocreate.gctl_flags = readonly ? G_GATE_FLAG_READONLY : 0;
  ggiocreate.gctl_maxcount = 0;
  ggiocreate.gctl_timeout = 0;
  if (*name != '\0') {
    ggiocreate.gctl_unit = G_GATE_NAME_GIVEN;
    strlcpy(ggiocreate.gctl_name, name, sizeof(ggiocreate.gctl_name));
  } else {
    ggiocreate.gctl_unit = G_GATE_UNIT_AUTO;
  }
  strlcpy(ggiocreate.gctl_info, info, sizeof(ggiocreate.gctl_info));
  if (ioctl(fd, G_GATE_CMD_CREATE, &ggiocreate) == -1) {
    err("failed to create " G_GATE_PROVIDER_NAME " device");
    goto fail;
  }

  debug(20, "%s: created, unit: %d, name: %s", __func__, ggiocreate.gctl_unit,
      ggiocreate.gctl_name);

  drv->fd = fd;
  drv->unit = ggiocreate.gctl_unit;
  *drv_ = drv;

  if (*name == '\0') {
    snprintf(name, namelen, "%s%d", G_GATE_PROVIDER_NAME, drv->unit);
  }

  return 0;

fail:
  free(drv);
fail_close:
  close(fd);
  return -errno;
}

void ggate_drv_destroy(ggate_drv_t drv_) {
  struct ggate_drv *drv = (struct ggate_drv *)drv_;
  struct g_gate_ctl_destroy ggiodestroy;

  debug(20, "%s %p", __func__, drv);

  memset(&ggiodestroy, 0, sizeof(ggiodestroy));
  ggiodestroy.gctl_version = G_GATE_VERSION;
  ggiodestroy.gctl_unit = drv->unit;
  ggiodestroy.gctl_force = 1;

  // Remember errno.
  int rerrno = errno;

  int r = ioctl(drv->fd, G_GATE_CMD_DESTROY, &ggiodestroy);
  if (r == -1) {
    err("failed to destroy /dev/%s%d device", G_GATE_PROVIDER_NAME,
        drv->unit);
  }
  // Restore errno.
  errno = rerrno;

  free(drv);
}

int ggate_drv_resize(ggate_drv_t drv_, size_t newsize) {
  struct ggate_drv *drv = (struct ggate_drv *)drv_;

  debug(20, "%s %p: newsize=%zd", __func__, drv, newsize);

  struct g_gate_ctl_modify ggiomodify;

  memset(&ggiomodify, 0, sizeof(ggiomodify));
  ggiomodify.gctl_version = G_GATE_VERSION;
  ggiomodify.gctl_unit = drv->unit;
  ggiomodify.gctl_modify = GG_MODIFY_MEDIASIZE;
  ggiomodify.gctl_mediasize = newsize;

  int r = ioctl(drv->fd, G_GATE_CMD_MODIFY, &ggiomodify);
  if (r == -1) {
    r = -errno;
    err("failed to resize /dev/%s%d device", G_GATE_PROVIDER_NAME, drv->unit);
  }
  return r;
}

int ggate_drv_kill(const char *devname) {
  debug(20, "%s %s", __func__, devname);

  int fd = open("/dev/" G_GATE_CTL_NAME, O_RDWR);
  if (fd == -1) {
    err("failed to open /dev/" G_GATE_CTL_NAME);
    return -errno;
  }

  struct g_gate_ctl_destroy ggiodestroy;
  memset(&ggiodestroy, 0, sizeof(ggiodestroy));
  ggiodestroy.gctl_version = G_GATE_VERSION;
  ggiodestroy.gctl_unit = G_GATE_NAME_GIVEN;
  ggiodestroy.gctl_force = 1;

  strlcpy(ggiodestroy.gctl_name, devname, sizeof(ggiodestroy.gctl_name));

  int r = ioctl(fd, G_GATE_CMD_DESTROY, &ggiodestroy);
  if (r == -1) {
    r = -errno;
    err("failed to destroy %s device", devname);
  }

  close(fd);
  return r;
}

int ggate_drv_recv(ggate_drv_t drv_, ggate_drv_req_t *req) {
  struct ggate_drv *drv = (struct ggate_drv *)drv_;
  struct g_gate_ctl_io *ggio;
  int error, r;

  debug(20, "%s", __func__);

  ggio = calloc(1, sizeof(*ggio));
  if (ggio == NULL) {
    return -ENOMEM;
  }

  ggio->gctl_version = G_GATE_VERSION;
  ggio->gctl_unit = drv->unit;
  ggio->gctl_data = malloc(MAXPHYS);
  ggio->gctl_length = MAXPHYS;

  debug(20, "%s: waiting for request from kernel",  __func__);
  if (ioctl(drv->fd, G_GATE_CMD_START, ggio) == -1) {
    err("%s: G_GATE_CMD_START failed", __func__);
    return -errno;
  }

  debug(20, "%s: got request from kernel: "
        "unit=%d, seq=%ju, cmd=%u, offset=%ju, length=%ju, error=%d, data=%p",
        __func__, ggio->gctl_unit, (uintmax_t)ggio->gctl_seq, ggio->gctl_cmd,
        (uintmax_t)ggio->gctl_offset, (uintmax_t)ggio->gctl_length,
        ggio->gctl_error, ggio->gctl_data);

  error = ggio->gctl_error;
  switch (error) {
  case 0:
    break;
  case ECANCELED:
    debug(10, "%s: canceled: exit gracefully",  __func__);
    r = -error;
    goto fail;
  case ENOMEM:
    /*
     * Buffer too small? Impossible, we allocate MAXPHYS
     * bytes - request can't be bigger than that.
     */
    /* FALLTHROUGH */
  case ENXIO:
  default:
    errno = error;
    err("%s: G_GATE_CMD_START failed", __func__);
    r = -error;
    goto fail;
  }

  *req = ggio;
  return 0;

fail:
  free(ggio->gctl_data);
  free(ggio);
  return r;
}

int ggate_drv_send(ggate_drv_t drv_, ggate_drv_req_t req) {
  struct ggate_drv *drv = (struct ggate_drv *)drv_;
  struct g_gate_ctl_io *ggio = (struct g_gate_ctl_io *)req;
  int r = 0;

  debug(20, "%s: send request to kernel: "
        "unit=%d, seq=%ju, cmd=%u, offset=%ju, length=%ju, error=%d, data=%p",
        __func__, ggio->gctl_unit, (uintmax_t)ggio->gctl_seq, ggio->gctl_cmd,
        (uintmax_t)ggio->gctl_offset, (uintmax_t)ggio->gctl_length,
        ggio->gctl_error, ggio->gctl_data);

  if (ioctl(drv->fd, G_GATE_CMD_DONE, ggio) == -1) {
    err("%s: G_GATE_CMD_DONE failed", __func__);
    r = -errno;
  }

  free(ggio->gctl_data);
  free(ggio);
  return r;
}

static const char * get_conf(struct ggeom *gp, const char *name) {
	struct gconfig *conf;

	LIST_FOREACH(conf, &gp->lg_config, lg_config) {
		if (strcmp(conf->lg_name, name) == 0)
			return (conf->lg_val);
	}
	return "";
}

int ggate_drv_list(struct ggate_drv_info *info, size_t *size) {
  struct gmesh mesh;
  struct gclass *class;
  struct ggeom *gp;
  int r;
  size_t max_size;

  r = geom_gettree(&mesh);
  if (r != 0) {
    return -errno;
  }

  max_size = *size;
  *size = 0;

  LIST_FOREACH(class, &mesh.lg_class, lg_class) {
    if (strcmp(class->lg_name, G_GATE_CLASS_NAME) == 0) {
      LIST_FOREACH(gp, &class->lg_geom, lg_geom) {
        (*size)++;
      }
      if (*size > max_size) {
        r = -ERANGE;
        goto done;
      }
      LIST_FOREACH(gp, &class->lg_geom, lg_geom) {
        strlcpy(info->id, get_conf(gp, "unit"), sizeof(info->id));
        strlcpy(info->name, gp->lg_name, sizeof(info->name));
        strlcpy(info->info, get_conf(gp, "info"), sizeof(info->info));
        info++;
      }
    }
  }

done:
  geom_deletetree(&mesh);
  return r;
}
