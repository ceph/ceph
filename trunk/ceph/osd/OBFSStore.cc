// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */



#include "OBFSStore.h"

extern "C" {
#include "../../uofs/uofs.h"
}

#include "common/Timer.h"

#include "include/types.h"

#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/file.h>
#include <iostream>
#include <cassert>
#include <errno.h>
#include <dirent.h>


#include "config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << "osd" << whoami << ".obfs "

OBFSStore::OBFSStore(int whoami, char *param, char *dev)
{
    this->whoami = whoami;
    this->mounted = -1;
    this->bdev_id = -1;
    this->param[0] = 0;
    this->dev[0] = 0;
    if (dev)
        strcpy(this->dev, dev);
    if (param) 
        strcpy(this->param, param);
}

int OBFSStore::mount(void)
{
    dout(0) << "OBFS init!" << endl;
    if ((this->bdev_id = device_open(this->dev, O_RDWR)) < 0) {
        dout(0) << "device open FAILED on " << this->dev << ", errno " << errno << endl;
        return -1;
    }

    this->mkfs();
    this->mounted = uofs_mount(this->bdev_id, 
                               g_conf.uofs_cache_size,
                               g_conf.uofs_min_flush_pages,
                               this->whoami);
    switch (this->mounted) {
        case -1:
            this->mkfs();
            //retry to mount
            dout(0) << "remount the OBFS" << endl;
            this->mounted = uofs_mount(this->bdev_id, 
                                       g_conf.uofs_cache_size,
                                       g_conf.uofs_min_flush_pages,
                                       this->whoami);
            assert(this->mounted >= 0);
            break;
        case -2: 
            //fsck
            dout(0) << "Need fsck! Simply formatted for now!" << endl;
            this->mkfs();
            this->mounted = uofs_mount(this->bdev_id, 
                                       g_conf.uofs_cache_size,
                                       g_conf.uofs_min_flush_pages,
                                       this->whoami);
            assert(this->mounted >= 0);
            break;
        case 0:
            //success
            break;
        default:
            break;
    }

    if (this->mounted >= 0) 
        dout(0) << "successfully mounted!" << endl;
    else
        dout(0) << "error in mounting obfsstore!" << endl;
    
    return 0;
}

int OBFSStore::mkfs(void)
{
  /*int    donode_size_byte     = 1024,
        bd_ratio                = 10,
        reg_size_mb             = 256,
        sb_size_kb              = 4,
        lb_size_kb              = 1024,
      nr_hash_table_buckets   = 1023,
      delay_allocation        = 1,
      flush_interval        = 5;
    FILE    *param;
  */
    
    
    if (this->mounted >= 0)
      return 0;

    dout(0) << "OBFS.mkfs!" << endl;
    /*
    if (strlen(this->param) > 0) {
        param = fopen(this->param, "r");
        if (param) {
            //fscanf(param, "Block Device: %s\n", this->dev);
            fscanf(param, "Donode Size: %d\n", &donode_size_byte);
            fscanf(param, "Block vs Donode Ratio: %d\n", &bd_ratio);
            fscanf(param, "Region Size: %d MB\n", &reg_size_mb);
            fscanf(param, "Small Block Size: %d KB\n", &sb_size_kb);
            fscanf(param, "Large Block Size: %d KB\n", &lb_size_kb);
            fscanf(param, "Hash Table Buckets: %d\n", &nr_hash_table_buckets);
            fscanf(param, "Delayed Allocation: %d\n", &delay_allocation);
        } else {
            dout(0) << "read open FAILED on "<< this->param <<", errno " << errno << endl;
            dout(0) << "use default parameters" << endl;
        }
    } else
        dout(0) << "use default parameters" << endl;
    */

    if (this->bdev_id <= 0)
        if ((this->bdev_id = device_open(this->dev, O_RDWR)) < 0) {
            dout(0) << "device open FAILED on "<< this->dev <<", errno " << errno << endl;
            return -1;
        }
    
    dout(0) << "start formating!" << endl;

    uofs_format(this->bdev_id,
                g_conf.uofs_onode_size, 
                g_conf.uofs_block_meta_ratio, 
                g_conf.uofs_segment_size,
                g_conf.uofs_small_block_size,
                g_conf.uofs_large_block_size,
                g_conf.uofs_nr_hash_buckets,
                g_conf.uofs_delay_allocation, 
                0,//g_conf.uofs_dev_force_size,
                g_conf.uofs_flush_interval, 
                0);

    dout(0) << "formatting complete!" << endl;
    return 0;
}

int OBFSStore::umount(void)
{
    uofs_shutdown();
    close(this->bdev_id);

    return 0;
}

int OBFSStore::statfs(struct statfs *sfs) 
{
  return 0;
}

bool OBFSStore::exists(object_t oid)
{
    //dout(0) << "calling function exists!" << endl;
    return uofs_exist(oid);
}

int OBFSStore::stat(object_t oid, struct stat *st)
{
    dout(0) << "calling function stat!" << endl;
    if (uofs_exist(oid)) return 0;
    return -1;
}

int OBFSStore::remove(object_t oid)
{
    dout(0) << "calling remove function!" << endl;
    return uofs_del(oid);
}

int OBFSStore::truncate(object_t oid, off_t size)
{
    dout(0) << "calling truncate function!" << endl;
    //return uofs_truncate(oid, size);
    return -1;
}

int OBFSStore::read(object_t oid, size_t len, 
            off_t offset, bufferlist &bl)
{
    //dout(0) << "calling read function!" << endl;
    //dout(0) << oid << " 0  " << len << " " << offset << " 100" << endl;

  // FIXME: page-align this and we can avoid a memcpy...
  bl.push_back(new buffer(len));
  return uofs_read(oid, bl.c_str(), offset, len);
}

int OBFSStore::write(object_t oid, size_t len,
                     off_t offset, bufferlist& bl, bool fsync)
{
    int ret = 0;
    
    //dout(0) << "calling write function!" << endl;
    //if (whoami == 0)
    //    dout(0) << oid << " 0  " << len << " " << offset << " 101" << endl;

    for (list<bufferptr>::iterator p = bl.buffers().begin();
         p != bl.buffers().end();
         p++) {
      ret += uofs_write(oid, (*p).c_str(), offset, len, 0);
    }

    if (fsync)
        ret += uofs_sync(oid);
    
    return ret;
}


int OBFSStore::write(object_t oid, size_t len,
             off_t offset, bufferlist& bl, Context *onflush)
{
  int r = write(oid, len, offset, bl, false);
  g_timer.add_event_after((float)g_conf.uofs_fake_sync, onflush);
  return r;
}
