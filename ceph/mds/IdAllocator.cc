
#include "include/config.h"
#undef dout
#define dout(x)  if (x <= g_conf.debug) cout << "idalloc:"

#define DBLEVEL  20

#include "IdAllocator.h"
#include "MDS.h"
#include "osd/Filer.h"

#include "include/types.h"

char ifn[100];


idno_t IdAllocator::get_id(int type) 
{
  assert(opened);

  dout(DBLEVEL) << "idalloc " << this << ": type " << type << " dump:" << endl;
  //free[type].dump();
  idno_t id = free[type].first();
  free[type].erase(id);
  dout(DBLEVEL) << "idalloc " << this << ": getid type " << type << " is " << id << endl;
  //free[type].dump();
  save();
  return id;
}

void IdAllocator::reclaim_id(int type, idno_t id) 
{
  assert(opened);

  dout(DBLEVEL) << "idalloc " << this << ": reclaim type " << type << " id " << id << endl;
  free[type].insert(id);
  //free[type].dump();
  save();
}




char *IdAllocator::get_filename() {
  sprintf(ifn,"osddata/idalloc.%d", mds->get_nodeid());
  return ifn;
}

void IdAllocator::save()
{
  crope data;

  int ntypes = free.size();
  data.append((char*)&ntypes, sizeof(ntypes));
	
  // over types
  for (map<int, rangeset<idno_t> >::iterator ty = free.begin();
	   ty != free.end(); 
	   ty++) {
	int type = ty->first;
	data.append((char*)&type, sizeof(type));
	  
	int mapsize = free[type].map_size();
	data.append((char*)&mapsize, sizeof(mapsize));
	dout(DBLEVEL) << "type " << type << " num " << mapsize << endl;
	  
	// over entries
	for (map<idno_t,idno_t>::iterator it = free[type].map_begin();
		 it != free[type].map_end();
		 it++) {
	  idno_t a = it->first;
	  idno_t b = it->second;
	  data.append((char*)&a, sizeof(a));
	  data.append((char*)&b, sizeof(b));
	  mapsize--;
	}
	assert(mapsize == 0);
  }

  // write (async)
  mds->filer->write(MDS_INO_IDS_OFFSET + mds->get_nodeid(),
					data.length(),
					0,
					data.c_str(),
					0,
					NULL);
}


class C_ID_Load : public Context {
public:
  IdAllocator *ida;
  char *dataptr;
  char *freeptr;
  C_ID_Load(IdAllocator *ida) {
	this->ida = ida;
  }
  void finish(int r) {
	ida->load_2(r, dataptr, freeptr);
  }
};

void IdAllocator::load()
{ 
  C_ID_Load *c = new C_ID_Load(this);

  assert(opened == false);
  assert(opening == false);
  opening = true;

  mds->filer->read(MDS_INO_IDS_OFFSET + mds->get_nodeid(),
				   1000000,
				   0,
				   &c->dataptr, &c->freeptr,
				   c);
}

void IdAllocator::load_2(int r, char *dataptr, char *freeptr)
{
  if (r > 0) {
	int off = 0;
	int ntypes = *(int*)(dataptr + off);
	off += sizeof(ntypes);
	
	dout(DBLEVEL) << "ntypes " << ntypes << endl;
	
	for (int ty = 0; ty < ntypes; ty++) {
	  int type = *(int*)(dataptr + off);
	  off += sizeof(type);
	  
	  int mapsize = *(int*)(dataptr + off);
	  off += sizeof(mapsize);
	  
	  dout(DBLEVEL) << "type " << type << " num " << mapsize << endl;
	  for (int i=0; i<mapsize; i++) {
		idno_t a = *(idno_t*)(dataptr + off);
		off += sizeof(a);
		idno_t b = *(idno_t*)(dataptr + off);
		off += sizeof(b);
		free[type].map_insert(a,b);
	  }
	  free[type].dump();
	}
  }
  else {
	// use generic range FIXME THIS IS CRAP
	free[ID_INO].map_insert((long long)1000000LL * (mds->get_nodeid()+1),
							(long long)1000000LL * (mds->get_nodeid()+2) - 1);
	//free[ID_INO].dump();
	
	free[ID_FH].map_insert(1000000LL * (mds->get_nodeid()+1),
						   1000000LL * (mds->get_nodeid()+2) - 1);
	//free[ID_FH].dump();
  }

  opened = true;
  opening = false;
}
