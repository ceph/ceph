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

#ifndef __THREAD_H
#define __THREAD_H

#include <pthread.h>

class Thread {
 private:
  pthread_t thread_id;

 public:
  Thread() : thread_id(0) {}

  pthread_t &get_thread_id() { return thread_id; }
  bool is_started() { return thread_id != 0; }

  virtual void *entry() = 0;

 private:
  static void *_entry_func(void *arg) {
	return ((Thread*)arg)->entry();
  }

 public:
  int create() {
	return pthread_create( &thread_id, NULL, _entry_func, (void*)this );
  }

  int join(void **prval = 0) {
	if (thread_id == 0) return -1;   // never started.
	int status = pthread_join(thread_id, prval);
	if (status == 0) 
	  thread_id = 0;
	else
	  cout << "join status = " << status << endl;
	return status;
  }
};

#endif
