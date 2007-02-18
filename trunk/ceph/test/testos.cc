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
  unsigned write_iter = 5;
  unsigned random_seed = ::time(NULL);

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
  strcpy (osd_file, "/tmp/testos.XXXXXX");
  mktemp (osd_file);

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
      os = new OSBDB (osd_file);
    }
  else if (strcasecmp (osd_name, "osbdb-btree") == 0)
    {
      g_conf.bdbstore_btree = true;
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

  unsigned long long totalw = 0;
  unsigned long long totalw_max = 0;
  unsigned long long totalw_min = 0;
  unsigned long long totalr = 0;
  unsigned long long totalr_max = 0;
  unsigned long long totalr_min = 0;
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

      struct timeval tm1, tm2;
      struct timezone tz;
      memset (&tz, 0, sizeof (tz));
      srandom (random_seed);

      unsigned long long write_us = 0;
      unsigned long long max_write_us = 0;
      unsigned long long min_write_us = 0xFFFFFFFF;
      char *val = (char *) valloc (object_size);
      auto_ptr<char> valptr (val);
      for (unsigned o = 0; o < object_count; o++)
        {
          object_t oid ((uint64_t) random() << 32 | random(), random());

          {
            long *v = (long *) val;
            int n = object_size / sizeof (long);
            for (int j = 0; j < n; j++)
              v[j] = random();
          }

          bufferptr bp (val, object_size);
          bufferlist bl;
          bl.push_back (bp);
          int ret;
          gettimeofday (&tm1, &tz);
          if ((ret = os->write (oid, 0L, object_size, bl, NULL)) < 0)
            cerr << "write " << oid << " failed: " << strerror (-ret) << endl;
          gettimeofday (&tm2, &tz);

          unsigned long long us = to_usec (tm2) - to_usec (tm1);
          write_us += us;
          max_write_us = MAX(max_write_us, us);
          min_write_us = MIN(min_write_us, us);
        }

      cerr << "Wrote in " << write_us << " us" << endl;
      cerr << "Average: " << (write_us / object_count)
           << "us; min: " << min_write_us
           << "us; max: " << max_write_us << " us" << endl;
      totalw = totalw + (write_us / object_count);
      totalw_max += max_write_us;
      totalw_min += min_write_us;

      srandom (random_seed);

      unsigned long long read_us = 0;
      unsigned long long max_read_us = 0;
      unsigned long long min_read_us = 0xFFFFFFFF;
      char *val2 = (char *) valloc(object_size);
      auto_ptr<char> valptr2(val2);
      for (unsigned o = 0; o < object_count; o++)
        {
          object_t oid ((uint64_t) random() << 32 | random(), random());
          
          {
            long *v = (long *) val;
            int n = object_size / sizeof (long);
            for (int j = 0; j < n; j++)
              v[j] = random();
          }

          bufferptr bp (val2, object_size);
          bufferlist bl;
          bl.push_back (bp);
          
          gettimeofday (&tm1, &tz);
          if (os->read (oid, 0L, object_size, bl) < 0)
            {
              cerr << "object " << oid << " not found!" << endl;
              continue;
            }
          gettimeofday (&tm2, &tz);

          unsigned long long us = to_usec (tm2) - to_usec (tm1);
          read_us += us;
          max_read_us = MAX(max_read_us, us);
          min_read_us = MIN(min_read_us, us);
          if (!memcmp (val, val2, object_size) != 0)
            cerr << "read doesn't match written!" << endl;
        }

      cerr << "Read in " << read_us << " ms" << endl;
      cerr << "Average: " << (read_us / object_count)
           << "us; min: " << min_read_us
           << "us; max: " << max_read_us << " us" << endl;
      totalr = totalr + (read_us / object_count);
      totalr_max += max_read_us;
      totalr_min += min_read_us;
      os->umount();
      random_seed++;
    }

  cerr << "Finished in " << ((totalr + totalw) / 1000) << " ms" << endl;
  cout << "TESTOS: write " << osd_name << ":" << object_size << ":"
       << object_count << ":" << write_iter << ":" << (random_seed - write_iter)
       << " -- " << (totalw / write_iter) << " " << (totalw_max / write_iter)
       << " " << (totalw_min / write_iter) << endl;
  cout << "TESTOS: read " << osd_name << ":" << object_size << ":"
       << object_count << ":" << write_iter << ":" << (random_seed - write_iter)
       << " -- " << (totalr / write_iter) << " " << (totalr_max / write_iter)
       << " " << (totalr_min / write_iter) << endl;

  unlink (osd_file);
  exit (0);
}
