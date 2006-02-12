// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */

#ifndef _OBFSSTORE_H_
#define _OBFSSTORE_H_

#include "ObjectStore.h"
#include "Fake.h"

class OBFSStore : public ObjectStore, 
				  public FakeStoreAttrs, 
				  public FakeStoreCollections {
  int	whoami;
  int	bdev_id;
  int	mounted;
  char	dev[128];
  char	param[128];
  
 public:
  OBFSStore(int whoami, char *param, char *dev);
  
  int mount(void);
  int umount(void);
  int mkfs(void);
  
  int statfs(struct statfs *);

  bool exists(object_t oid);
  int stat(object_t oid, struct stat *st);
  
  int remove(object_t oid);
  int truncate(object_t oid, off_t size);
  
  int read(object_t oid, size_t len, 
		   off_t offset, bufferlist& bl);
  int write(object_t oid, size_t len, 
			off_t offset, bufferlist& bl,
			bool fsync);
  int write(object_t oid, size_t len, 
			off_t offset, bufferlist& bl,
			Context *onflush);
  
};

#endif
