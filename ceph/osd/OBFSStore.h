
#ifndef _OBFSSTORE_H_
#define _OBFSSTORE_H_

#include "ObjectStore.h"

class OBFSStore: public ObjectStore {
	int	whoami;
	int	bdev_id;
	int	mounted;
	char	dev[128];
	char	param[128];

      public:
	OBFSStore(int whoami, char *param, char *dev);

	int init(void);
	int finalize(void);
	int mkfs(void);

	bool exists(object_t oid);
	int stat(object_t oid, struct stat *st);

	int remove(object_t oid);
	int truncate(object_t oid, off_t size);

	int read(object_t oid, size_t len, 
		 off_t offset, char *buffer);
	int write(object_t oid, size_t len, 
		  off_t offset,char *buffer,
		  bool fsync);

};

#endif
