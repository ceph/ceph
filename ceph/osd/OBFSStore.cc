
#include "OBFSStore.h"
#include "../include/uofs.h"
#include "../include/types.h"

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
	this->param[0] = 0;
	if (dev)
		strcpy(this->dev, dev);
	if (param) 
		strcpy(this->param, param);
}

int OBFSStore::init(void)
{
	int	dev_id;

	if ((this->bdev_id = device_open(this->dev, 0)) < 0) {
		dout(1) << "device open FAILED on " << this->dev << ", errno " << errno << endl;
		return -1;
	}

	uofs_mount(dev_id);

	return 0;
}

int OBFSStore::mkfs(void)
{
	int	donode_size_byte 	= 1024,
		bd_ratio                = 10,
		reg_size_mb             = 256,
		sb_size_kb              = 4,
		lb_size_kb              = 512,
		nr_hash_table_buckets   = 1023,
		delay_allocation        = 0,
		flush_interval		= 5;
		bdev_id;
	FILE	*param;
	
	if (strlen(this->param) > 0) {
		param = fopen(this->param, "r");
		if (param) {
			fscanf(param, "Block Device: %s\n", this->dev);
			fscanf(param, "Donode Size: %d\n", &donode_size_byte);
			fscanf(param, "Block vs Donode Ratio: %d\n", &bd_ratio);
			fscanf(param, "Region Size: %d MB\n", &reg_size_mb);
			fscanf(param, "Small Block Size: %d KB\n", &sb_size_kb);
			fscanf(param, "Large Block Size: %d KB\n", &lb_size_kb);
			fscanf(param, "Hash Table Buckets: %d\n", &nr_hash_table_buckets);
			fscanf(param, "Delayed Allocation: %d\n", &delay_allocation);
		} else {
			dout(1) << "read open FAILED on "<< this->param <<", errno " << errno << endl;
			dout(1) << "use default parameters" << endl;
		}
	}

	if ((bdev_id = device_open(this->dev, 0)) < 0) {
		dout(1) << "device open FAILED on "<< this->dev <<", errno " << errno << endl;
		return -1;
	}

	uofs_format(bdev_id, donode_size_byte, bd_ratio, reg_size_mb, sb_size_kb, 
		    lb_size_kb, nr_hash_table_buckets, delay_allocation, flush_interval);

	close(bdev_id);

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
	return uofs_exist(oid);
}

int OBFSStore::stat(object_t oid, struct stat *st)
{
}

int OBFSStore::remove(object_t oid)
{
	return uofs_del(oid);
}

int OBFSStore::truncate(object_t oid, off_t size)
{
	//return uofs_truncate(oid, size);
}

int OBFSStore::read(object_t oid, size_t len, 
		    off_t offset, char *buffer)
{
	return uofs_read(oid, buffer, offset, len);
}

int OBFSStore::write(object_t oid, size_t len,
		     off_t offset, char *buffer, bool fsync)
{
	int ret;
	
	ret = uofs_write(oid, buffer, offset, len);
	if (fsync)
		ret += uofs_sync(oid);
	
	return ret;
}


