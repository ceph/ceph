// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2010 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 * Simple little program to let you:
 * 1) View the layout information on a file or directory,
 * 2) Modify the layout information on an empty file,
 * 3) Modify the default layout on a directory
 */

#include <iostream>
#include <string.h>
#include <errno.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include "client/ioctl.h"

using namespace std;


#define CMD_SHOW_LAYOUT 1
#define CMD_SHOW_LOC    2
#define CMD_SET_LAYOUT  3

void usage();
int init_options(int argc, char **argv, int *fd, char **path, int *cmd,
                 int *stripe_unit, int *stripe_count,
                 int *object_size, int *pool, int* osd, int *file_offset, bool *dir);
int get_layout(int fd, struct ceph_ioctl_layout *layout);
int get_location(int fd, struct ceph_ioctl_dataloc *location);


int main (int argc, char **argv) {
  int fd = 0;
  int err = 0;
  char *path = 0;
  int cmd = 0;
  int stripe_unit = 0;
  int stripe_count = 0;
  int object_size = 0;
  int pool = 0;
  int osd = 0;
  int file_offset = 0;
  bool dir = false;

  if (init_options(argc, argv, &fd, &path, &cmd, &stripe_unit, &stripe_count,
                   &object_size, &pool, &osd, &file_offset, &dir)){
    usage();
    return 0;
  }

  if (CMD_SHOW_LAYOUT == cmd) {
    struct ceph_ioctl_layout layout;
    memset(&layout, 0, sizeof(layout));
    err = ioctl(fd, CEPH_IOC_GET_LAYOUT, (unsigned long)&layout);
    if (err) {
      cerr << "Error getting layout: "
	   << (err == -1 ? strerror(errno) : strerror(-err)) << endl;
      return 1;
    }
    if (layout.stripe_unit == 0) {
      cerr << "layout not specified" << endl;
    } else {
      cout << "layout.data_pool:     " << layout.data_pool << endl;
      cout << "layout.object_size:   " << layout.object_size << endl;
      cout << "layout.stripe_unit:   " << layout.stripe_unit << endl;
      cout << "layout.stripe_count:  " << layout.stripe_count << endl;
      cout << "layout.preferred_osd: " << layout.preferred_osd << endl;
    }
  } else if (CMD_SHOW_LOC == cmd) {
    struct ceph_ioctl_dataloc location;
    location.file_offset = file_offset;
    err = ioctl(fd, CEPH_IOC_GET_DATALOC, (unsigned long)&location);
    if (err) {
      cerr << "Error getting location: "
	   << (err == -1 ? strerror(errno) : strerror(-err)) << endl;
      return 1;
    }
    cout << "location.file_offset:  " << location.file_offset << endl;
    cout << "location.object_offset:" << location.object_offset << endl;
    cout << "location.object_no:    " << location.object_no << endl;
    cout << "location.object_size:  " << location.object_size << endl;
    cout << "location.object_name:  " << location.object_name << endl;
    cout << "location.block_offset: " << location.block_offset << endl;
    cout << "location.block_size:   " << location.block_size << endl;
    cout << "location.osd:          " << location.osd << endl;
//    cout << "osd address:           " << location.osd_addr << endl;
  } else if (CMD_SET_LAYOUT == cmd) {
    struct ceph_ioctl_layout layout;
    memset(&layout, 0, sizeof(layout));
    int ioctl_num = (dir ? CEPH_IOC_SET_LAYOUT_POLICY : CEPH_IOC_SET_LAYOUT);
    layout.data_pool = pool;
    layout.object_size = object_size;
    layout.preferred_osd = osd;
    layout.stripe_count = stripe_count;
    layout.stripe_unit = stripe_unit;
    err = ioctl(fd, ioctl_num, (unsigned long)&layout);
    if (err) {
      cerr << "Error setting layout: " 
	   << (err == -1 ? strerror(errno) : strerror(-err)) << endl;
      return 1;
    }
  } else {
    cerr << "unknown cmd somehow set!" << endl;
    usage();
    return 1;
  }

  return 0;
}


void usage() {
  cerr << "usage: cephfs path command [options]*" << endl;
  cerr << "Commands:" << endl;
  cerr << "   show_layout    -- view the layout information on a file or dir" << endl;
  cerr << "   set_layout     -- set the layout on an empty file,\n"
       << "                     or the default layout on a directory" << endl;
  cerr << "   show_location  -- view the location information on a file" << endl;
  cerr << "Options:" << endl;
  cerr << "   Useful for setting layouts:" << endl;
  cerr << "   --stripe_unit, -u:  set the size of each stripe" << endl;
  cerr << "   --stripe_count, -c: set the number of stripes per object" << endl;
  cerr << "   --object_size, -s:  set the size of the objects to stripe across" << endl;
  cerr << "   --pool, -p:         set the pool to use" << endl;
  cerr << "   --osd, -o:          set the preferred osd to use as primary" << endl;
  cerr << endl;
  cerr << "   Useful for getting location data:" << endl;
  cerr << "   --offset, -l:       the offset to retrieve location data for" << endl;
  cerr << endl;
}

int init_options(int argc, char **argv, int *fd, char **path, int *cmd,
                 int *stripe_unit, int *stripe_count,
                 int *object_size, int *pool, int* osd, int *file_offset,
                 bool *dir) {
  // look through the options, make sure they're valid,
  // and set the variables from them
  int i = 3;
  struct stat stat_field;

  if (argc < 3) {
    cerr << "not enough parameters!" << endl;
    return 1;
  }

  *path = argv[1];

  *fd = open(argv[1], O_RDONLY);
  if (*fd < 0) {
    cerr << "error opening path: " << strerror(*fd) << endl;
  }

  if (!strcmp(argv[2], "show_layout")) {
    *cmd = CMD_SHOW_LAYOUT;
    if (argc > 4)
      return 1;
  } else if (!strcmp(argv[2], "set_layout")) {
    *cmd = CMD_SET_LAYOUT;
  } else if (!strcmp(argv[2], "show_location")){
    *cmd = CMD_SHOW_LOC;
  } else {
    cerr << "invalid command" << endl;
    return 1;
  }

  // okay, fill in options
  while (i < argc) {
    if (i == argc-1) {
      // there's an option without an associated value!
      cerr << "not all options are paired with a value!" << endl;
      return 1;
    }

    if (!strcmp(argv[i], "--stripe_unit") || argv[i][1] == 'u') {
      if (*cmd != CMD_SET_LAYOUT) {
        cerr << "Invalid option for command!" << endl;
        return 1;
      }
      *stripe_unit = atoi(argv[i+1]);
      if (!*stripe_unit) {
        cerr << "invalid value for stripe unit" << endl;
        return 1;
      }
    } else if (!strcmp(argv[i], "--stripe_count") || argv[i][1] == 'c') {
      if (*cmd != CMD_SET_LAYOUT) {
        cerr << "Invalid option for command!" << endl;
        return 1;
      }
      *stripe_count = atoi(argv[i+1]);
      if (!*stripe_count) {
        cerr << "invalid value for stripe count" << endl;
        return 1;
      }
    } else if (!strcmp(argv[i], "--object_size") || argv[i][1] == 's') {
      if (*cmd != CMD_SET_LAYOUT) {
        cerr << "Invalid option for command!" << endl;
        return 1;
      }
      *object_size = atoi(argv[i+1]);
      if (!*object_size) {
        cerr << "invalid value for object size" << endl;
        return 1;
      }
    } else if (!strcmp(argv[i], "--pool") || argv[i][1] == 'p') {
      if (*cmd != CMD_SET_LAYOUT) {
        cerr << "Invalid option for command!" << endl;
        return 1;
      }
      *pool= atoi(argv[i+1]);
      if (!*pool) {
        cerr << "invalid value for pool" << endl;
        return 1;
      }
    } else if (!strcmp(argv[i], "--osd") || argv[i][1] == 'o') {
      if (*cmd != CMD_SET_LAYOUT) {
        cerr << "Invalid option for command!" << endl;
        return 1;
      }
      *osd = atoi(argv[i+1]);
      if (!*osd) {
        cerr << "invalid value for osd" << endl;
        return 1;
      }
    } else if (!strcmp(argv[i], "--offset") || argv[i][1] == 'l') {
      if (*cmd != CMD_SHOW_LOC) {
        cerr << "Invalid option for command!" << endl;
        return 1;
      }
      *file_offset = atoi(argv[i+1]);
      if (!*file_offset) {
        cerr << "invalid value for offset" << endl;
        return 1;
      }
    }
    i += 2;
  }

  fstat (*fd, &stat_field);
  if (S_ISREG(stat_field.st_mode)) { // open read-write to set layout
    close(*fd);
    *fd = open(argv[1], O_RDWR);
    if (*fd < 0) {
      cerr << "error opening file" << endl;
      return 1;
    }
  } else {
    *dir = true;
  }

  return 0;
}
