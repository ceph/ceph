/* testos.cc -- simple ObjectStore test harness.
   Copyright (C) 2007 Casey Marshall <csm@soe.ucsc.edu>

Ceph - scalable distributed file system

This is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License version 2.1, as published by the Free Software 
Foundation.  See file COPYING. */


#include "osd/ObjectStore.h"
#include "ebofs/Ebofs.h"
#include "osbdb/OSBDB.h"
#include "include/buffer.h"

#include <iostream>
#include <cerrno>

#include <fcntl.h>
#include <sys/mount.h>

using namespace std;

static inline unsigned long long
to_usec (struct timeval &time)
{
  return (((unsigned long long) time.tv_sec * 1000000)
          + ((unsigned long long) time.tv_usec));
}

static inline unsigned long long
to_msec (struct timeval &time)
{
  return (((unsigned long long) time.tv_sec * 1000)
          + ((unsigned long long) time.tv_usec / 1000));
}

int main (int argc, char **argv)
{
  char *osd_name = "ebofs";
  unsigned object_size = 1024;
  unsigned object_count = 1024;
  unsigned write_iter = 64;
  unsigned random_seed = ::time(NULL);
  char *device = "/tmp/testos";
  char *mountcmd = "mount /tmp/testos";
  char *umountcmd = "umount /tmp/testos";

  bool inhibit_remount = (getenv("TESTOS_INHIBIT_REMOUNT") != NULL);

  if (argc > 1
      && (strcmp (argv[1], "-h") == 0
          || strcmp (argv[1], "-help") == 0
          || strcmp (argv[1], "--help") == 0
          || argc > 6))
    {
      cout << "usage: " << argv[0] << " [store [object-size [object-count [iterations [seed]]]]]" << endl;
      cout << endl;
      cout << "Where the arguments are:" << endl << endl;
      cout << "  store -- store type; default \"ebofs\"" << endl;
      cout << "  object-size -- size of objects; default 1024" << endl;
      cout << "  object-count -- number of objects to write; default 1024"
           << endl;
      cout << "  iterations -- write the objects that many times; default 5"
           << endl;
      cout << "  seed -- random seed; default current time" << endl;
      exit (0);
    }

  if (argc > 1)
    osd_name = argv[1];
  if (argc > 2)
    object_size = (unsigned) atol (argv[2]);
  if (argc > 3)
    object_count = (unsigned) atol (argv[3]);
  if (argc > 4)
    write_iter = (unsigned) atol (argv[4]);
  if (argc > 5)
    random_seed = (unsigned) atol (argv[5]);

  // algin object size to 'long'
  object_size = ((object_size + (sizeof (long) - 1)) / sizeof (long)) * sizeof (long);

  char *osd_file = new char[32];
  strcpy (osd_file, "/tmp/testos/testos.XXXXXX");
  mktemp (osd_file);

  if (!inhibit_remount)
    {
      if (system (mountcmd) != 0)
        {
          cerr << "mount failed" << endl;
          exit (1);
        }
    }
      
  ObjectStore *os = NULL;
  if (strcasecmp (osd_name, "ebofs") == 0)
    {
      FILE *f = fopen (osd_file, "w");
      if (f == NULL)
        {
          cerr << "failed to open " << osd_file << ": " << strerror (errno)
               << endl;
          exit (1);
        }
      // 1G file.
      fseek (f, 1024 * 1024 * 1024, SEEK_SET);
      fputc ('\0', f);
      fclose (f);
      // 20K cache
      g_conf.ebofs_bc_size = 5; // times 4K
      os = new Ebofs (osd_file);
    }
  else if (strcasecmp (osd_name, "osbdb") == 0)
    {
      char *e = getenv ("OSBDB_FFACTOR");
      if (e != NULL)
        g_conf.bdbstore_ffactor = atol(e);
      e = getenv ("OSBDB_NELEM");
      if (e != NULL)
        g_conf.bdbstore_nelem = atol(e);
      e = getenv ("OSBDB_PAGESIZE");
      if (e != NULL)
        g_conf.bdbstore_pagesize = atol(e);
      g_conf.debug_bdbstore = 1;
      // 20K cache
      g_conf.bdbstore_cachesize = 20 * 1024;
      os = new OSBDB (osd_file);
    }
  else if (strcasecmp (osd_name, "osbdb-btree") == 0)
    {
      g_conf.bdbstore_btree = true;
      // 20K cache
      g_conf.bdbstore_cachesize = 20 * 1024;
      os = new OSBDB (osd_file);
    }
  else
    {
      cerr << "I don't know about object store \"" << osd_name << "\""
           << endl;
      exit (1);
    }

  cout << "Writing " << object_count << " objects of size "
       << object_size << " to " << osd_name << endl;

  char *val = (char *) malloc (object_size);
  char *val2 = (char *) malloc (object_size);
  auto_ptr<char> valptr (val);
  auto_ptr<char> valptr2(val2);
  if (getenv ("TESTOS_UNALIGNED") != NULL)
    {
      val = val + 1;
      val2 = val2 + 1;
    }

  for (unsigned i = 0; i < object_size; i++)
    {
      val[i] = (char) i;
      val2[i] = (char) i;
    }
  object_t *oids = new object_t[object_count];

  utime_t writes[write_iter];
  utime_t total_write;
  utime_t reads[write_iter];
  utime_t total_read;
  for (unsigned i = 0; i < write_iter; i++)
    {
      cerr << "Iteration " << i << endl;

      int ret = os->mkfs();
      if (ret != 0)
        {
          cerr << "mkfs(" << osd_file << "): " << strerror (-ret) << endl;
          exit (1);
        }
      ret = os->mount();
      if (ret != 0)
        {
          cerr << "mount(): " << strerror (-ret) << endl;
          exit (1);
        }

      srandom (random_seed + i);

      for (unsigned j = 0; j < object_count; j++)
        {
          oids[j].ino = (uint64_t) random() << 32 | random();
          oids[j].bno = random();
        }

      utime_t begin = g_clock.now();
      for (unsigned o = 0; o < object_count; o++)
        {
          bufferptr bp (val, object_size);
          bufferlist bl;
          bl.push_back (bp);
          int ret;
          if ((ret = os->write (oids[o], 0L, object_size, bl, NULL)) < 0)
            cerr << "write " << oids[o] << " failed: "
                 << strerror (-ret) << endl;
        }
      utime_t end = g_clock.now() - begin;

      cerr << "Write finished in " << end << endl;
      total_write += end;
      writes[i] = end;

      os->sync();
      os->umount();
      sync();

      if (!inhibit_remount)
        {
          if (system (umountcmd) != 0)
            {
              cerr << "umount failed" << endl;
              exit (1);
            }
      
          if (system (mountcmd) != 0)
            {
              cerr << "mount(2) failed" << endl;
              exit (1);
            }
        }
      
      os->mount();

      begin = g_clock.now();
      for (unsigned o = 0; o < object_count; o++)
        {
          bufferptr bp (val2, object_size);
          bufferlist bl;
          bl.push_back (bp);
          
          if (os->read (oids[o], 0L, object_size, bl) < 0)
            {
              cerr << "object " << oids[o] << " not found!" << endl;
            }
        }
      end = g_clock.now() - begin;

      cerr << "Read finished in " << end << endl;
      total_read += end;
      reads[i] = end;

      os->umount();
      sync();

      if (!inhibit_remount)
        {
          if (system (umountcmd) != 0)
            {
              cerr << "umount(2) failed" << endl;
              exit (1);
            }
      
          if (system (mountcmd) != 0)
            {
              cerr << "mount(3) failed" << endl;
              exit (1);
            }
        }      
    }

  cerr << "Finished in " << (total_write + total_read) << endl;

  double write_mean = (double) total_write / write_iter;
  double write_sd = 0.0;
  for (unsigned i = 0; i < write_iter; i++)
    {
      double x = (double) writes[i] - write_mean;
      write_sd += x * x;
    }
  write_sd = sqrt (write_sd / write_iter);

  double read_mean = (double) total_read / write_iter;
  double read_sd = 0.0;
  for (unsigned i = 0; i < write_iter; i++)
    {
      double x = (double) reads[i] - read_mean;
      write_sd += x * x;
    }
  read_sd = sqrt (read_sd / write_iter);

  cout << "TESTOS: write " << osd_name << ":" << object_size << ":"
       << object_count << ":" << write_iter << ":" << random_seed
       << " -- " << write_mean << " " << write_sd << endl;

  cout << "TESTOS: read " << osd_name << ":" << object_size << ":"
       << object_count << ":" << write_iter << ":" << random_seed
       << " -- " << read_mean << " " << read_sd << endl;

  unlink (osd_file);
  if (!inhibit_remount)
    {
      if (system (umountcmd) != 0)
        {
          cerr << "umount(3) failed" << endl;
          exit (1);
        }
    }
  exit (0);
}
