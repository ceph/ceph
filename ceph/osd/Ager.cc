
#include "include/types.h"

#include "Ager.h"
#include "ObjectStore.h"

#include "config.h"
#include "common/Clock.h"


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
  static char buf[1024*1024];
  bufferlist bl;
  bl.push_back(new buffer(buf, 1024*1024));
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
	  ssize_t t = MIN(s, 1024*1024);
	  store->write(oid, off, t, bl, false);
	  off += t;
	  s -= t;
	}
	oid++;
  }
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


void Ager::age(int time,
			   float high_water,    // fill to this %
			   float low_water,     // then empty to this %
			   int count,         // this many times
			   float final_water,   // and end here ( <= low_water)
			   int fake_size_mb) { 
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
  
  for (int c=1; c<=count; c++) {
	if (g_clock.now() > until) break;
	
	dout(1) << "age " << c << "/" << count << " filling to " << high_water << endl;
	age_fill(high_water, until);
	if (c == count) {
	  dout(1) << "age final empty to " << final_water << endl;
	  age_empty(final_water);	
	} else {
	  dout(1) << "age " << c << "/" << count << " emptying to " << low_water << endl;
	  age_empty(low_water);
	}
  }
  store->sync();
  store->sync();
  dout(1) << "age finished" << endl;
}  

