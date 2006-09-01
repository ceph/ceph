
#include "include/types.h"

#include "Ager.h"
#include "ObjectStore.h"

#include "config.h"
#include "common/Clock.h"

// ick
#include "ebofs/Ebofs.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


object_t Ager::age_get_oid() {
  if (!age_free_oids.empty()) {
	object_t o = age_free_oids.front();
	age_free_oids.pop_front();
	return o;
  }
  return age_cur_oid++;
}

ssize_t Ager::age_pick_size() {
  ssize_t max = file_size_distn.sample() * 1024;
  return max/2 + (rand() % 100) * max/200 + 1;
}

void Ager::age_fill(float pc, utime_t until) {
  int max = 1024*1024;
  char *buf = new char[max];
  bufferlist bl;
  bl.push_back(new buffer(buf, max));
  while (1) {
	if (g_clock.now() > until) break;
	
	struct statfs st;
	store->statfs(&st);
	float a = (float)(st.f_blocks-st.f_bavail) / (float)st.f_blocks;
	//dout(10) << "age_fill at " << a << " / " << pc << " .. " << st.f_blocks << " " << st.f_bavail << endl;
	if (a >= pc) {
	  dout(2) << "age_fill at " << a << " / " << pc << " stopping" << endl;
	  break;
	}
	
	object_t oid = age_get_oid();
	
	int b = rand() % 10;
	age_objects[b].push_back(oid);
	
	ssize_t s = age_pick_size();
	
	dout(2) << "age_fill at " << a << " / " << pc << " creating " << hex << oid << dec << " sz " << s << endl;
	
	off_t off = 0;
	while (s) {
	  ssize_t t = MIN(s, max);
	  bufferlist sbl;
	  sbl.substr_of(bl, 0, t);
	  store->write(oid, off, t, sbl, false);
	  off += t;
	  s -= t;
	}
	oid++;
  }
  delete[] buf;
}

void Ager::age_empty(float pc) {
  int nper = 20;
  int n = nper;
  while (1) {
	struct statfs st;
	store->statfs(&st);
	float a = (float)(st.f_blocks-st.f_bfree) / (float)st.f_blocks;
	dout(2) << "age_empty at " << a << " / " << pc << endl;//" stopping" << endl;
	if (a <= pc) {
	  dout(2) << "age_empty at " << a << " / " << pc << " stopping" << endl;
	  break;
	}
	
	int b = rand() % 10;
	n--;
	if (n == 0 || age_objects[b].empty()) {
	  dout(2) << "age_empty sync" << endl;
	  //sync();
	  sync();
	  n = nper;
	  continue;
	}
	object_t oid = age_objects[b].front();
	age_objects[b].pop_front();
	
	dout(2) << "age_empty at " << a << " / " << pc << " removing " << hex << oid << dec << endl;
	
	store->remove(oid);
	age_free_oids.push_back(oid);
  }
}

void pfrag(ObjectStore::FragmentationStat &st)
{
  cout << st.num_extent << " avg " << st.avg_extent
	   << ", " << st.avg_extent_per_object << " per obj, " 
	   << st.avg_extent_jump << " jump, "
	   << st.num_free_extent << " free avg " << st.avg_free_extent;
  
  /*
	for (map<int,int>::iterator p = st.free_extent_dist.begin();
	p != st.free_extent_dist.end();
	p++) 
	cout << "\t" << p->first << "=\t" << p->second;
	cout << endl;
  */
  
  int n = st.num_free_extent;
  for (__uint64_t i=2; i <= 30; i += 2) {
	cout << "\t" 
	  //<< i
	  //<< "=\t" 
		 << st.free_extent_dist[i];
	n -= st.free_extent_dist[i];
	if (n == 0) break;
  }
}


void Ager::age(int time,
			   float high_water,    // fill to this %
			   float low_water,     // then empty to this %
			   int count,         // this many times
			   float final_water,   // and end here ( <= low_water)
			   int fake_size_mb) { 

  store->_fake_writes(true);

  utime_t until = g_clock.now();
  until.sec_ref() += time;
  
  while (age_objects.size() < 10) age_objects.push_back( list<object_t>() );
  
  if (fake_size_mb) {
	int fake_bl = fake_size_mb * 256;
	struct statfs st;
	store->statfs(&st);
	float f = (float)fake_bl / (float)st.f_blocks;
	high_water = (float)high_water * f;
	low_water = (float)low_water * f;
	final_water = (float)final_water * f;
	dout(2) << "fake " << fake_bl << " / " << st.f_blocks << " is " << f << ", high " << high_water << " low " << low_water << " final " << final_water << endl;
  }
  
  // init size distn (once)
  if (!did_distn) {
	did_distn = true;
	age_cur_oid = 1;
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

  for (int c=1; c<=count; c++) {
	if (g_clock.now() > until) break;
	
	dout(1) << "age " << c << "/" << count << " filling to " << high_water << endl;
	age_fill(high_water, until);
	store->sync();
	store->_get_frag_stat(st);
	pfrag(st);


	if (c == count) {
	  dout(1) << "age final empty to " << final_water << endl;
	  age_empty(final_water);	
	} else {
	  dout(1) << "age " << c << "/" << count << " emptying to " << low_water << endl;
	  age_empty(low_water);
	}
	store->sync();
	store->_get_frag_stat(st);
	pfrag(st);

  }

  // dump the freelist
  save_freelist();

  // ok!
  store->_fake_writes(false);
  store->sync();
  store->sync();
  dout(1) << "age finished" << endl;
}  


void Ager::load_freelist()
{
  struct stat st;
  
  int r = ::stat("ebofs.freelist", &st);
  assert(r == 0);

  bufferptr bp = new buffer(st.st_size);
  bufferlist bl;
  bl.push_back(bp);
  int fd = ::open("ebofs.freelist", O_RDONLY);
  ::read(fd, bl.c_str(), st.st_size);
  ::close(fd);

  ((Ebofs*)store)->_import_freelist(bl);
  store->sync();
  store->sync();
}

void Ager::save_freelist()
{
  bufferlist bl;
  ((Ebofs*)store)->_export_freelist(bl);
  ::unlink("ebofs.freelist");
  int fd = ::open("ebofs.freelist", O_CREAT|O_WRONLY);
  ::write(fd, bl.c_str(), bl.length());
  ::close(fd);
}
