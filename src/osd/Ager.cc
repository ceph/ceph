// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "include/types.h"

#include "Ager.h"
#include "os/ObjectStore.h"

#include "config.h"
#include "common/Clock.h"

// ick
//#include "ebofs/Ebofs.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#ifdef DARWIN
#include <sys/param.h>
#include <sys/mount.h>
#endif // DARWIN


int myrand() 
{
  if (0) 
    return rand();
  else {
    static int n = 0;
    srand(n++);
    return rand();
  }
}


file_object_t Ager::age_get_oid() {
  if (!age_free_oids.empty()) {
    file_object_t o = age_free_oids.front();
    age_free_oids.pop_front();
    return o;
  }
  file_object_t last = age_cur_oid;
  ++age_cur_oid.bno;
  return last;
}

ssize_t Ager::age_pick_size() {
  ssize_t max = file_size_distn.sample() * 1024;
  return max/2 + (myrand() % 100) * max/200 + 1;
}

bool start_debug = false;

uint64_t Ager::age_fill(float pc, utime_t until) {
  int max = 1024*1024;
  bufferptr bp(max);
  bp.zero();
  bufferlist bl;
  bl.push_back(bp);
  uint64_t wrote = 0;
  while (1) {
    if (g_clock.now() > until) break;
    
    struct statfs st;
    store->statfs(&st);
    float free = 1.0 - ((float)(st.f_bfree) / (float)st.f_blocks);
    float avail = 1.0 - ((float)(st.f_bavail) / (float)st.f_blocks);  // to write to
    //float a = (float)(st.f_bfree) / (float)st.f_blocks;
    //dout(10) << "age_fill at " << a << " / " << pc << " .. " << st.f_blocks << " " << st.f_bavail << dendl;
    if (free >= pc) {
      generic_dout(2) << "age_fill at " << free << " / " << avail << " / " << " / " << pc << " stopping" << dendl;
      break;
    }

    // make sure we can write to it..
    if (avail > .98 ||
        avail - free > .02) 
      store->sync();

    file_object_t poid = age_get_oid();
    
    int b = myrand() % 10;
    age_objects[b].push_back(poid);
    
    ssize_t s = age_pick_size();
    wrote += (s + 4095) / 4096;




    generic_dout(2) << "age_fill at " << free << " / " << avail << " / " << pc << " creating " << hex << poid << dec << " sz " << s << dendl;
    

    if (false && !g_conf.ebofs_verify && start_debug && wrote > 1000000ULL) { 
      /*


      1005700
?
1005000
1005700
      1005710
      1005725ULL
      1005750ULL
      1005800
      1006000

//  99  1000500 ? 1000750 1006000
*/
      g_conf.debug_ebofs = 30;
      g_conf.ebofs_verify = true;      
    }

    off_t off = 0;
    while (s) {
      ssize_t t = MIN(s, max);
      bufferlist sbl;
      sbl.substr_of(bl, 0, t);
      ObjectStore::Transaction tr;
      sobject_t oid(poid, 0);
      tr.write(coll_t(), oid, off, t, sbl);
      store->apply_transaction(tr);
      off += t;
      s -= t;
    }
    poid.bno++;
  }

  return wrote*4; // KB
}

void Ager::age_empty(float pc) {
  int nper = 20;
  int n = nper;

  //g_conf.ebofs_verify = true;

  while (1) {
    struct statfs st;
    store->statfs(&st);
    float free = 1.0 - ((float)(st.f_bfree) / (float)st.f_blocks);
    float avail = 1.0 - ((float)(st.f_bavail) / (float)st.f_blocks);  // to write to
    generic_dout(2) << "age_empty at " << free << " / " << avail << " / " << pc << dendl;//" stopping" << dendl;
    if (free <= pc) {
      generic_dout(2) << "age_empty at " << free << " / " << avail << " / " << pc << " stopping" << dendl;
      break;
    }
    
    int b = myrand() % 10;
    n--;
    if (n == 0 || age_objects[b].empty()) {
      generic_dout(2) << "age_empty sync" << dendl;
      //sync();
      //sync();
      n = nper;
      continue;
    }
    file_object_t poid = age_objects[b].front();
    age_objects[b].pop_front();
    
    generic_dout(2) << "age_empty at " << free << " / " << avail << " / " << pc << " removing " << hex << poid << dec << dendl;
    
    ObjectStore::Transaction t;
    sobject_t oid(poid, 0);
    t.remove(coll_t(), oid);
    store->apply_transaction(t);
    age_free_oids.push_back(poid);
  }

  g_conf.ebofs_verify = false;
}

void pfrag(uint64_t written, ObjectStore::FragmentationStat &st)
{
  cout << "#gb wr\ttotal\tn x\tavg x\tavg per\tavg j\tfree\tn fr\tavg fr\tnum<2\tsum<2\tnum<4\tsum<4\t..." 
       << std::endl;
  cout << written
       << "\t" << st.total
       << "\t" << st.num_extent
       << "\t" << st.avg_extent
       << "\t" << st.avg_extent_per_object
       << "\t" << st.avg_extent_jump 
       << "\t" << st.total_free
       << "\t" << st.num_free_extent
       << "\t" << st.avg_free_extent;
    
  int n = st.num_extent;
  for (uint64_t i=1; i <= 30; i += 1) {
    cout << "\t" << st.extent_dist[i];
    cout << "\t" << st.extent_dist_sum[i];
    //cout << "\ta " << (st.extent_dist[i] ? (st.extent_dist_sum[i] / st.extent_dist[i]):0);
    n -= st.extent_dist[i];
    if (n == 0) break;
  }
  cout << std::endl;
}


void Ager::age(int time,
               float high_water,    // fill to this %
               float low_water,     // then empty to this %
               int count,         // this many times
               float final_water,   // and end here ( <= low_water)
               int fake_size_mb) { 

  store->_fake_writes(true);
  srand(0);

  utime_t start = g_clock.now();
  utime_t until = start;
  until.sec_ref() += time;
  
  //int elapsed = 0;
  int freelist_inc = 60;
  utime_t nextfl = start;
  nextfl.sec_ref() += freelist_inc;

  while (age_objects.size() < 10) age_objects.push_back( list<file_object_t>() );
  
  if (fake_size_mb) {
    int fake_bl = fake_size_mb * 256;
    struct statfs st;
    store->statfs(&st);
    float f = (float)fake_bl / (float)st.f_blocks;
    high_water = (float)high_water * f;
    low_water = (float)low_water * f;
    final_water = (float)final_water * f;
    generic_dout(2) << "fake " << fake_bl << " / " << st.f_blocks << " is " << f << ", high " << high_water << " low " << low_water << " final " << final_water << dendl;
  }
  
  // init size distn (once)
  if (!did_distn) {
    did_distn = true;
    age_cur_oid = file_object_t(888, 0);
    file_size_distn.add(1, 19.0758125+0.65434375);
    file_size_distn.add(512, 35.6566);
    file_size_distn.add(1024, 27.7271875);
    file_size_distn.add(2*1024, 16.63503125);
    //file_size_distn.add(4*1024, 106.82384375);
    //file_size_distn.add(8*1024, 81.493375);
    //file_size_distn.add(16*1024, 14.13553125);
    //file_size_distn.add(32*1024, 2.176);
    //file_size_distn.add(256*1024, 0.655938);
    //file_size_distn.add(512*1024, 0.1480625);
    //file_size_distn.add(1*1024*1024, 0.020125); // actually 2, but 32bit
    file_size_distn.normalize();
  }
  
  // clear
  for (int i=0; i<10; i++)
    age_objects[i].clear();
  
  ObjectStore::FragmentationStat st;

  uint64_t wrote = 0;

  for (int c=1; c<=count; c++) {
    if (g_clock.now() > until) break;
    
    //if (c == 7) start_debug = true;
    
    generic_dout(1) << "#age " << c << "/" << count << " filling to " << high_water << dendl;
    uint64_t w = age_fill(high_water, until);
    //dout(1) << "age wrote " << w << dendl;
    wrote += w;
    //store->sync();
    //store->_get_frag_stat(st);
    //pfrag(st);


    if (c == count) {
      generic_dout(1) << "#age final empty to " << final_water << dendl;
      age_empty(final_water);    
    } else {
      generic_dout(1) << "#age " << c << "/" << count << " emptying to " << low_water << dendl;
      age_empty(low_water);
    }
    //store->sync();
    //store->sync();

    // show frag state
    store->_get_frag_stat(st);
    pfrag(wrote / (1024ULL*1024ULL) ,  // GB
          st);

    // dump freelist?
    /*
    if (g_clock.now() > nextfl) {
      elapsed += freelist_inc;
      save_freelist(elapsed);
      nextfl.sec_ref() += freelist_inc;
    }
    */
  }

  // dump the freelist
  //save_freelist(0);
  exit(0);   // hack

  // ok!
  store->_fake_writes(false);
  store->sync();
  store->sync();
  generic_dout(1) << "age finished" << dendl;
}  


void Ager::load_freelist()
{
  generic_dout(1) << "load_freelist" << dendl;

  struct stat st;
  
  int r = ::stat("ebofs.freelist", &st);
  assert(r == 0);

  bufferptr bp(st.st_size);
  bufferlist bl;
  bl.push_back(bp);
  int fd = ::open("ebofs.freelist", O_RDONLY);
  if (fd >= 0) {
    ::read(fd, bl.c_str(), st.st_size);
    ::close(fd);
  }

  //((Ebofs*)store)->_import_freelist(bl);
  store->sync();
  store->sync();
}


/*void Ager::save_freelist(int el)
{
  generic_dout(1) << "save_freelist " << el << dendl;
  char s[100];
  snprintf(s, sizeof(s), "ebofs.freelist.%d", el);
  bufferlist bl;
  ((Ebofs*)store)->_export_freelist(bl);
  ::unlink(s);
  int fd = ::open(s, O_CREAT|O_WRONLY, 0644);
  ::fchmod(fd, 0644);
  ::write(fd, bl.c_str(), bl.length());
  ::close(fd);
}
*/
