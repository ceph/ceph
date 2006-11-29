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



#include <iostream>
#include "ebofs/Ebofs.h"

bool stop = false;


int nt = 0;
class Tester : public Thread {
  Ebofs &fs;
  int t;
  
  char b[1024*1024];

public:
  Tester(Ebofs &e) : fs(e), t(nt) { nt++; }
  void *entry() {

    while (!stop) {
      object_t oid;
      oid.ino = (rand() % 2) + 0x10000000;
      coll_t cid = rand() % 50;
      off_t off = rand() % 10000;//0;//rand() % 1000000;
      off_t len = 1+rand() % 100000;
      char *a = "one";
      if (rand() % 2) a = "two";
      int l = 3;//rand() % 10;

      switch (rand() % 9) {
      case 0:
        {
          cout << t << " read " << hex << oid << dec << " at " << off << " len " << len << endl;
          bufferlist bl;
          fs.read(oid, off, len, bl);
          int l = MIN(len,bl.length());
          if (l) {
            cout << t << " got " << l << endl;
            bl.copy(0, l, b);
            char *p = b;
            while (l--) {
              assert(*p == 0 ||
                     *p == (char)(off ^ oid.ino));
              off++;
              p++;
            }
          }
        }
        break;

      case 1:
        {
          cout << t << " write " << hex << oid << dec << " at " << off << " len " << len << endl;
          for (int j=0;j<len;j++) 
            b[j] = (char)(oid.ino^(off+j));
	  bufferptr wp(b, len);
          bufferlist w;
          w.append(wp);
          fs.write(oid, off, len, w, 0);
        }
        break;

      case 2:
        cout << t << " remove " << hex << oid << dec <<  endl;
        fs.remove(oid);
        break;

      case 3:
        cout << t << " collection_add " << hex << oid << dec <<  " to " << cid << endl;
        fs.collection_add(cid, oid, 0);
        break;

      case 4:
        cout << t << " collection_remove " << hex << oid << dec <<  " from " << cid << endl;
        fs.collection_remove(cid, oid, 0);
        break;

      case 5:
        cout << t << " setattr " << hex << oid << dec <<  " " << a << " len " << l << endl;
        fs.setattr(oid, a, (void*)a, l, 0);
        break;
        
      case 6:
        cout << t << " rmattr " << hex << oid << dec <<  " " << a << endl;
        fs.rmattr(oid,a);
        break;

      case 7:
        {
          char v[4];
          cout << t << " getattr " << hex << oid << dec <<  " " << a << endl;
          if (fs.getattr(oid,a,(void*)v,3) == 0) {
            v[3] = 0;
            assert(strcmp(v,a) == 0);
          }
        }
        break;
        
      case 8:
        {
          cout << t << " truncate " << hex << oid << dec <<  " " << off << endl;
          fs.truncate(oid, 0);
        }
        break;
      }


    }
    cout << t << " done" << endl;
    return 0;
  }
};

int main(int argc, char **argv)
{
  vector<char*> args;
  argv_to_vec(argc, argv, args);
  parse_config_options(args);

  // args
  if (args.size() != 3) return -1;
  char *filename = args[0];
  int seconds = atoi(args[1]);
  int threads = atoi(args[2]);

  cout << "dev " << filename << " .. " << threads << " threads .. " << seconds << " seconds" << endl;

  Ebofs fs(filename);
  if (fs.mount() < 0) return -1;

  list<Tester*> ls;
  for (int i=0; i<threads; i++) {
    Tester *t = new Tester(fs);
    t->create();
    ls.push_back(t);
  }

  utime_t now = g_clock.now();
  utime_t dur(seconds,0);
  utime_t end = now + dur;
  cout << "stop at " << end << endl;
  while (now < end) {
    sleep(1);
    now = g_clock.now();
    cout << now << endl;
  }

  cout << "stopping" << endl;
  stop = true;
  
  while (!ls.empty()) {
    Tester *t = ls.front();
    ls.pop_front();
    t->join();
    delete t;
  }

  fs.umount();
  return 0;
}

