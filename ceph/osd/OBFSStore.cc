
#include "OBFSStore.h"

extern "C" {
#include "../../uofs/uofs.h"
}

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


#include "include/config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << "osd" << whoami << ".obfsstore "

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

int OBFSStore::init(void)
{
	dout(0) << "OBFS init!" << endl;
	if ((this->bdev_id = device_open(this->dev, O_RDWR)) < 0) {
		dout(0) << "device open FAILED on " << this->dev << ", errno " << errno << endl;
		return -1;
	}

	this->mkfs();
	this->mounted = uofs_mount(this->bdev_id, this->whoami);
	switch (this->mounted) {
		case -1:
			this->mkfs();
			//retry to mount
			dout(0) << "remount the OBFS" << endl;
			this->mounted = uofs_mount(this->bdev_id, this->whoami);
			assert(this->mounted >= 0);
			break;
		case -2: 
			//fsck
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
	int	donode_size_byte 	= 1024,
		bd_ratio                = 10,
		reg_size_mb             = 256,
		sb_size_kb              = 4,
		lb_size_kb              = 1024,
		nr_hash_table_buckets   = 1023,
		delay_allocation        = 1,
		flush_interval		= 5;
	FILE	*param;
	

	if (this->mounted >= 0)
		return 0;

	dout(0) << "OBFS.mkfs!" << endl;
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

	if (this->bdev_id <= 0)
		if ((this->bdev_id = device_open(this->dev, O_RDWR)) < 0) {
			dout(0) << "device open FAILED on "<< this->dev <<", errno " << errno << endl;
			return -1;
		}
	
	dout(0) << "start formating!" << endl;

	uofs_format(this->bdev_id, donode_size_byte, bd_ratio, (reg_size_mb << 20), (sb_size_kb << 10), 
			(lb_size_kb << 10), nr_hash_table_buckets, delay_allocation, flush_interval, this->whoami);

	dout(0) << "formatting complete!" << endl;
	return 0;
}

int OBFSStore::finalize(void)
{
	uofs_shutdown();
	close(this->bdev_id);

	return 0;
}

bool OBFSStore::exists(object_t oid)
{
	dout(0) << "calling function exists!" << endl;
	return uofs_exist(oid);
}

int OBFSStore::stat(object_t oid, struct stat *st)
{
	dout(0) << "calling function stat!" << endl;
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
}

int OBFSStore::read(object_t oid, size_t len, 
		    off_t offset, char *buffer)
{
	//dout(0) << "calling read function!" << endl;
	//dout(0) << oid << " 0  " << len << " " << offset << " 100" << endl;
	return uofs_read(oid, buffer, offset, len);
}

int OBFSStore::write(object_t oid, size_t len,
		     off_t offset, char *buffer, bool fsync)
{
	int ret;//, sync = 0;
	
	//dout(0) << "calling write function!" << endl;
	//dout(0) << oid << " 0  " << len << " " << offset << " 101" << endl;
	//if (fsync) sync = 1;
	ret = uofs_write(oid, buffer, offset, len, 0);
	//if (fsync)
	//	ret += uofs_sync(oid);
	
	return ret;
}


