// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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



#include <iostream>
#include "ebofs/Ebofs.h"


int main(int argc, const char **argv)
{
  // args
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  parse_config_options(args);

  if (args.size() < 1) {
    cerr << "usage: mkfs.ebofs [options] <device file>" << std::endl;
    return -1;
  }
  const char *filename = args[0];

  // mkfs
  Ebofs mfs(filename);
  int r = mfs.mkfs();
  if (r < 0) exit(r);

  if (args.size() > 1) {   // pass an extra arg of some sort to trigger the test crapola
    // test-o-rama!
    Ebofs fs(filename);
    fs.mount();

    // zillion objects
    if (1) {
      char crap[1024*1024];
      memset(crap, 0, 1024*1024);
      bufferlist bl;
      int sz = 10000;  
      bl.append(crap, sz);
      
      int n = 100000;
      utime_t start = g_clock.now();
      for (int i=0; i<n; i++) {
	if (i && i % 1000 == 0) {
	  utime_t now = g_clock.now();
	  utime_t end = now;
	  end -= start;
	  start = now;
	  cout << i << " / " << n << " in " << end << std::endl;
	}
	pobject_t poid(0, 0, object_t(i,0));
	fs.write(0, poid, 0, sz, bl, (Context*)0);
      }
    }

    // streaming write test
    if (0) {
      char crap[1024*1024];
      memset(crap, 0, 1024*1024);
      

      pobject_t oid(0, 0, object_t(1,2));
      uint64_t pos = 0;
      uint64_t sz = 16;

      bufferlist bl;
      bl.append(crap, sz);

      struct timespec ts;
      ts.tv_sec = 0;
      ts.tv_nsec = 1000*1000*40;  // ms -> nsec

      while (1) {
	cout << g_clock.now() << " writing " << pos << "~" << sz << std::endl;
	fs.write(0, oid, pos, sz, bl, (Context*)0);
	pos += sz;
	nanosleep(&ts, 0);
      }

    }

    /*
    if (1) {
      // partial write tests
      char crap[1024*1024];
      memset(crap, 0, 1024*1024);

      bufferlist small;
      small.append(crap, 10);
      bufferlist med;
      med.append(crap, 1000);
      bufferlist big;
      big.append(crap, 1024*1024);

      cout << "0" << std::endl;
      fs.write(10, 0, 1024*1024, big, (Context*)0);
      fs.sync();
      fs.trim_buffer_cache();

      cout << "1" << std::endl;
      fs.write(10, 10, 10, small, 0);
      fs.write(10, 1, 1000, med, 0);
      fs.sync();
      fs.trim_buffer_cache();

      cout << "2" << std::endl;
      fs.write(10, 10, 10, small, 0);
      //fs.sync();
      fs.write(10, 1, 1000, med, 0);
      fs.sync();
      fs.trim_buffer_cache();

      cout << "3" << std::endl;
      fs.write(10, 1, 1000, med, 0);
      fs.write(10, 10000, 10, small, 0);
      fs.truncate(10, 100, 0);
      fs.sync();
      fs.trim_buffer_cache();

      cout << "4" << std::endl;
      fs.remove(10);
      fs.sync();
      fs.write(10, 10, 10, small, 0);
      fs.sync();
      fs.write(10, 1, 1000, med, 0);
      fs.sync();
      fs.truncate(10, 100, 0);
      fs.write(10, 10, 10, small, 0);
      fs.trim_buffer_cache();

      

    }

    if (0) { // onode write+read test
      bufferlist bl;
      char crap[1024*1024];
      memset(crap, 0, 1024*1024);
      bl.append(crap, 10);

      fs.write(10, 10, 0, bl, (Context*)0);
      fs.umount();

      Ebofs fs2(filename);
      fs2.mount();
      fs2.read(10, 10, 0, bl);
      fs2.umount();

      return 0;
    }


    if (0) {  // small write + read test
      bufferlist bl;
      char crap[1024*1024];
      memset(crap, 0, 1024*1024);

      object_t oid = 10;
      int n = 10000;
      int l = 128;
      bl.append(crap, l);


      char *p = bl.c_str();
      uint64_t o = 0;
      for (int i=0; i<n; i++) {
        cout << "write at " << o << std::endl;
        for (int j=0;j<l;j++) 
          p[j] = (char)(oid^(o+j));
        fs.write(oid, l, o, bl, (Context*)0);
        o += l;
      }

      fs.sync();
      fs.trim_buffer_cache();

      o = 0;
      for (int i=0; i<n; i++) {
        cout << "read at " << o << std::endl;
        bl.clear();
        fs.read(oid, l, o, bl);
        
        char b[l];
        bl.copy(0, l, b);
        char *p = b;
        int left = l;
        while (left--) {
          assert(*p == (char)(o ^ oid));
          o++;
          p++;
        }
      }

    }

    if (0) { // big write speed test
      bufferlist bl;
      char crap[1024*1024];
      memset(crap, 0, 1024*1024);
      bl.append(crap, 1024*1024);
      
      int megs = 1000;

      utime_t start = g_clock.now();

      for (uint64_t m=0; m<megs; m++) {
        //if (m%100 == 0)
          cout << m << " / " << megs << std::endl;
        fs.write(10, bl.length(), 1024LL*1024LL*m, bl, (Context*)0);
      }      
      fs.sync();

      utime_t end = g_clock.now();
      end -= start;

      cout << "elapsed " << end << std::endl;
      
      float mbs = (float)megs / (float)end;
      cout << "mb/s " << mbs << std::endl;
    }
    
    if (0) {  // test
      bufferlist bl;
      char crap[10000];
      memset(crap, 0, 10000);
      bl.append(crap, 10000);
      fs.write(10, bl.length(), 200, bl, (Context*)0);
      fs.trim_buffer_cache();
      fs.write(10, bl.length(), 5222, bl, (Context*)0);
      sleep(1);
      fs.trim_buffer_cache();
      fs.write(10, 5000, 3222, bl, (Context*)0);
    }
    
    // test small writes
    if (0) {
      char crap[1024*1024];
      memset(crap, 0, 1024*1024);
      bufferlist bl;
      bl.append(crap, 1024*1024);
      
      // reandom write
      if (1) {
        srand(0);
        for (int i=0; i<10000; i++) {
          uint64_t off = rand() % 1000000;
          size_t len = 1+rand() % 10000;
          cout << std::endl << i << " writing bit at " << off << " len " << len << std::endl;
          fs.write(10, len, off, bl, (Context*)0);
          //fs.sync();
          //fs.trim_buffer_cache();
        }
        fs.remove(10);
        for (int i=0; i<100; i++) {
          uint64_t off = rand() % 1000000;
          size_t len = 1+rand() % 10000;
          cout << std::endl << i << " writing bit at " << off << " len " << len << std::endl;
          fs.write(10, len, off, bl, (Context*)0);
          //fs.sync();
          //fs.trim_buffer_cache();
        }
      }
      
      if (0) {
        // sequential write
        srand(0);
        uint64_t off = 0;
        for (int i=0; i<10000; i++) {
          size_t len = 1024*1024;//1+rand() % 10000;
          cout << std::endl << i << " writing bit at " << off << " len " << len << std::endl;
          fs.write(10, len, off, bl, (Context*)0);
          off += len;
        }

      }
      
      
      if (0) {
        // read
        srand(0);
        for (int i=0; i<100; i++) {
          bufferlist bl;
          uint64_t off = rand() % 1000000;
          size_t len = rand() % 1000;
          cout << std::endl << "read bit at " << off << " len " << len << std::endl;
          int r = fs.read(10, len, off, bl);
          assert(bl.length() == len);
          assert(r == (int)len);
        }
      }
      
      // flush
      fs.sync();
      fs.trim_buffer_cache();
      //fs.trim_buffer_cache();
      
      if (0) {
        // read again
        srand(0);
        for (int i=0; i<100; i++) {
          bufferlist bl;
          uint64_t off = rand() % 1000000;
          size_t len = 100;
          cout << std::endl << "read bit at " << off << " len " << len << std::endl;
          int r = fs.read(10, len, off, bl);
          assert(bl.length() == len);
          assert(r == (int)len);
        }
        
        // flush
        fs.sync();
        fs.trim_buffer_cache();
      }
      
      if (0) {
        // write on empty cache
        srand(0);
        for (int i=0; i<100; i++) {
          uint64_t off = rand() % 1000000;
          size_t len = 100;
          cout << std::endl <<  "writing bit at " << off << " len " << len << std::endl;
          fs.write(10, len, off, bl, (Context*)0);
        }
      }
      
    }
    */
    
    fs.sync();
    fs.trim_buffer_cache();
    
    fs.umount();
  }

  return 0;
}

    
