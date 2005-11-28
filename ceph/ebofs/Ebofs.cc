
#include "Ebofs.h"

// *******************

#undef dout
#define dout(x) if (x <= g_conf.debug) cout << "ebofs."

int Ebofs::mount()
{
  assert(!mounted);

  // read super
  bufferptr bp1, bp2;
  dev.read(0, 1, bp1);
  dev.read(1, 1, bp2);

  struct ebofs_super *sb1 = (struct ebofs_super*)bp1.c_str();
  struct ebofs_super *sb2 = (struct ebofs_super*)bp2.c_str();
  struct ebofs_super *sb = 0;

  // pick newest super
  dout(2) << "mount super @0 v " << sb1->version << endl;
  dout(2) << "mount super @1 v " << sb2->version << endl;
  if (sb1->version > sb2->version)
	sb = sb1;
  else
	sb = sb2;

  super_version = sb->version;

  // init node pools
  dout(2) << "mount table nodepool" << endl;
  table_nodepool.read( dev, &sb->table_nodepool );
  
  // open tables
  dout(2) << "mount opening tables" << endl;
  object_tab = new Table<object_t, Extent>( table_nodepool, sb->object_tab );
  for (int i=0; i<EBOFS_NUM_FREE_BUCKETS; i++)
	free_tab[i] = new Table<block_t, block_t>( table_nodepool, sb->free_tab[i] );

  dout(2) << "mount mounted" << endl;
  mounted = true;
  return 0;
}


int Ebofs::mkfs()
{
  block_t num_blocks = dev.get_num_blocks();

  // create first noderegion
  Extent nr;
  nr.start = 2;
  nr.length = num_blocks / 10000;
  if (nr.length < 10) nr.length = 10;
  NodeRegion *r = new NodeRegion(0,nr);
  table_nodepool.add_region(r);
  table_nodepool.init_all_free();
  dout(1) << "mkfs: first node region at " << nr << endl;
  
  // init tables
  struct ebofs_table empty;
  empty.num_keys = 0;
  empty.root = -1;
  empty.depth = 0;

  object_tab = new Table<object_t, Extent>( table_nodepool, empty );
  collection_tab = new Table<coll_t, Extent>( table_nodepool, empty );
  
  for (int i=0; i<EBOFS_NUM_FREE_BUCKETS; i++)
	free_tab[i] = new Table<block_t,block_t>( table_nodepool, empty );

 
  // add free space
  Extent left;
  left.start = nr.start + nr.length;
  left.length = num_blocks - left.start;
  dout(1) << "mkfs: free blocks at " << left << endl;
  allocator.release( left );

  // write nodes
  dout(1) << "mkfs: flushing nodepool" << endl;
  table_nodepool.induce_full_flush();
  table_nodepool.flush( dev );
  
  // write super (2x)
  dout(1) << "mkfs: writing superblocks" << endl;
  super_version = 0;
  write_super();
  write_super();
  
  mounted = true;
  return 0;
}

int Ebofs::umount()
{
  mounted = false;

  // wait 

  // flush
  dout(1) << "umount: flushing nodepool" << endl;
  table_nodepool.flush( dev );

  // super
  dout(1) << "umount: writing superblocks" << endl;
  write_super();

  // close tables
  delete object_tab;
  delete collection_tab;
  for (int i=0; i<EBOFS_NUM_FREE_BUCKETS; i++)
	delete free_tab[i];

  return 0;
}



int Ebofs::write_super()
{
  struct ebofs_super sb;

  super_version++;
  block_t bno = super_version % 2;

  dout(1) << "write_super v" << super_version << " to b" << bno << endl;

  // fill in super
  sb.s_magic = EBOFS_MAGIC;
  sb.version = super_version;
  sb.num_blocks = dev.get_num_blocks();
  sb.free_blocks = free_blocks;
  //sb.num_objects = num_objects;


  // tables
  sb.object_tab.num_keys = object_tab->get_num_keys();
  sb.object_tab.root = object_tab->get_root();
  sb.object_tab.depth = object_tab->get_depth();

  for (int i=0; i<EBOFS_NUM_FREE_BUCKETS; i++) {
	sb.free_tab[i].num_keys = free_tab[i]->get_num_keys();
	sb.free_tab[i].root = free_tab[i]->get_root();
	sb.free_tab[i].depth = free_tab[i]->get_depth();
  }

  // pools
  sb.table_nodepool.num_regions = table_nodepool.get_num_regions();
  for (int i=0; i<table_nodepool.get_num_regions(); i++) {
	sb.table_nodepool.region_loc[i] = table_nodepool.get_region_loc(i);
  }

  char *c = new char[EBOFS_BLOCK_SIZE*2];
  bufferptr bp = new buffer(c + (EBOFS_BLOCK_SIZE-((unsigned)c%EBOFS_BLOCK_SIZE)), 
							EBOFS_BLOCK_SIZE, 
							BUFFER_MODE_NOFREE|BUFFER_MODE_NOCOPY);
  memcpy(bp.c_str(), (const char*)&sb, sizeof(sb));
  dev.write(bno, 1, bp);
  delete[] c;
  return 0;
}




// *** onodes ***

Onode* Ebofs::new_onode(object_t oid)
{
  Onode* on = new Onode(oid);

  assert(onode_map.count(oid) == 0);
  onode_map[oid] = on;
  onode_lru.lru_insert_mid(on);
  
  object_tab->insert( oid, on->onode_loc );  // even tho i'm not placed yet

  on->get();
  return on;
}


Onode* Ebofs::get_onode(object_t oid)
{
  // in cache?
  if (onode_map.count(oid)) {
	// yay
	Onode *on = onode_map[oid];
	on->get();
	return on;   
  }

  // on disk?
  Extent onode_loc;
  if (object_tab->lookup(oid, onode_loc) != Table<object_t,Extent>::Cursor::MATCH) {
	// object dne.
	return 0;
  }

  // read it!
  bufferlist bl;
  bufferpool.alloc_list( onode_loc.length, bl );
  dev.read( onode_loc.start, onode_loc.length, bl );

  // parse data block
  Onode *on = new Onode(oid);

  struct ebofs_onode *eo = (struct ebofs_onode*)bl.c_str();
  on->onode_loc = eo->onode_loc;
  on->object_size = eo->object_size;
  on->object_blocks = eo->object_blocks;

  // parse attributes
  char *p = bl.c_str() + sizeof(*eo);
  for (int i=0; i<eo->num_attr; i++) {
	string key = p;
	p += key.length() + 1;
	int len = *(int*)(p);
	p += sizeof(len);
	void *val = new char[len];
	memcpy(val, p, len);
	p += len;
	on->attr[key] = pair<int,void*>(len, val);
  }

  // parse extents
  on->extents.clear();
  for (int i=0; i<eo->num_extents; i++) {
	on->extents.push_back( *(Extent*)(p) );
	p += sizeof(Extent);
  }

  return on;
}


void Ebofs::write_onode(Onode *on)
{
  // allocate
  int bytes = sizeof(ebofs_onode) + on->get_attr_bytes() + on->get_extent_bytes();
  unsigned blocks = (bytes-1)/EBOFS_BLOCK_SIZE + 1;

  bufferlist bl;
  bufferpool.alloc_list( blocks, bl );

  // place on disk    
  if (on->onode_loc.length < blocks) {
	// relocate onode!
	if (on->onode_loc.length) 
	  allocator.release(on->onode_loc);

	block_t first = 0;
	if (on->extents.size()) 
	  first = on->extents[0].start;
	
	allocator.allocate(on->onode_loc, blocks, first);
	object_tab->remove( on->object_id );
	object_tab->insert( on->object_id, on->onode_loc );
  }
  
  struct ebofs_onode *eo = (struct ebofs_onode*)bl.c_str();
  eo->onode_loc = on->onode_loc;
  eo->object_id = on->object_id;
  eo->object_size = on->object_size;
  eo->object_blocks = on->object_blocks;
  eo->num_attr = on->attr.size();
  eo->num_extents = on->extents.size();
  
  // attr
  char *p = bl.c_str() + sizeof(*eo);
  for (map<string, pair<int, void*> >::iterator i = on->attr.begin();
	   i != on->attr.end();
	   i++) {
	memcpy(p, i->first.c_str(), i->first.length()+1);
	p += i->first.length()+1;
	*(int*)(p) = i->second.first;
	p += sizeof(int);
	memcpy(p, i->second.second, i->second.first);
	p += i->second.first;
  }

  // extents
  for (unsigned i=0; i<on->extents.size(); i++) {
	*(Extent*)(p) = on->extents[i];
	p += sizeof(Extent);
  }

  // write
  dev.write( on->onode_loc.start, on->onode_loc.length, bl );
}

void Ebofs::remove_onode(Onode *on)
{
  // remove from table
  object_tab->remove(on->object_id);

  // free onode space
  if (on->onode_loc.length)
	allocator.release(on->onode_loc);

  // free data space
  for (unsigned i=0; i<on->extents.size(); i++)
	allocator.release(on->extents[i]);

  delete on;
}

void Ebofs::put_onode(Onode *on)
{
  on->put();
}





// *** file i/o ***

int Ebofs::read(object_t oid, size_t len, off_t off, bufferlist& bl)
{
  

}
