
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
  memset(&sb, 0, sizeof(sb));
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

  bufferptr bp = bufferpool.alloc();
  memcpy(bp.c_str(), (const char*)&sb, sizeof(sb));
  dev.write(bno, 1, bp);
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
	on->attr[key] = OnodeAttrVal(p, len);
  }

  // parse extents
  on->extents.clear();
  for (int i=0; i<eo->num_extents; i++) {
	on->extents.push_back( *(Extent*)(p) );
	p += sizeof(Extent);
  }

  on->get();
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
  
  struct ebofs_onode eo;
  eo.onode_loc = on->onode_loc;
  eo.object_id = on->object_id;
  eo.object_size = on->object_size;
  eo.object_blocks = on->object_blocks;
  eo.num_attr = on->attr.size();
  eo.num_extents = on->extents.size();
  bl.copy_in(0, sizeof(eo), (char*)&eo);
  
  // attr
  unsigned off = sizeof(eo);
  for (map<string, OnodeAttrVal >::iterator i = on->attr.begin();
	   i != on->attr.end();
	   i++) {
	bl.copy_in(off, i->first.length()+1, i->first.c_str());
	off += i->first.length()+1;
	bl.copy_in(off, sizeof(int), (char*)&i->second.len);
	off += sizeof(int);
	bl.copy_in(off, i->second.len, i->second.data);
	off += i->second.len;
  }
  
  // extents
  for (unsigned i=0; i<on->extents.size(); i++) {
	bl.copy_in(off, sizeof(Extent), (char*)&on->extents[i]);
	off += sizeof(Extent);
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



void Ebofs::trim_onode_cache()
{
  while (onode_lru.lru_get_size() > onode_lru.lru_get_max()) {
	// expire an item
	Onode *on = (Onode*)onode_lru.lru_expire();
	if (on == 0) break;  // nothing to expire

	// expire
	dout(12) << "trim_onode_cache removing " << on->object_id << endl;
	onode_map.erase(on->object_id);
	delete on;
  }

  dout(10) << "trim_onode_cache " << onode_lru.lru_get_size() << " left" << endl;
}


// *** buffer cache ***

void Ebofs::trim_buffer_cache()
{
  bc.lock.Lock();

  // flush any dirty items?
  while (bc.lru_dirty.lru_get_size() > bc.lru_dirty.lru_get_max()) {
	BufferHead *bh = (BufferHead*) bc.lru_dirty.lru_expire();
	if (!bh) break;

	bc.lru_dirty.lru_insert_bot(bh);

	dout(10) << "trim_buffer_cache dirty " << *bh << endl;
	assert(bh->is_dirty());
	
	Onode *on = get_onode( bh->oc->get_object_id() );
	bh_write(on, bh);
	put_onode(on);
  }
  
  // trim bufferheads
  while (bc.lru_rest.lru_get_size() > bc.lru_rest.lru_get_max()) {
	BufferHead *bh = (BufferHead*) bc.lru_rest.lru_expire();
	if (!bh) break;

	dout(10) << "trim_buffer_cache rest " << *bh << endl;
	assert(bh->is_clean());
	
	ObjectCache *oc = bh->oc;
	oc->remove_bh(bh);
	delete bh;
	
	if (oc->is_empty()) {
	  Onode *on = get_onode( oc->get_object_id() );
	  dout(10) << "trim_buffer_cache closing oc on " << *on << endl;
	  on->close_oc();
	  put_onode(on);
	}
  }
  dout(10) << "trim_buffer_cache " 
		   << bc.lru_rest.lru_get_size() << " rest + " 
		   << bc.lru_dirty.lru_get_size() << " dirty " << endl;

  bc.lock.Unlock();
}

class C_E_Flush : public Context {
  int *i;
  Mutex *m;
  Cond *c;
public:
  C_E_Flush(int *_i, Mutex *_m, Cond *_c) : i(_i), m(_m), c(_c) {}
  void finish(int r) {
	(*i)--;
	m->Lock();
	c->Signal();
	m->Unlock();
  }
};

void Ebofs::flush_all()
{
  // FIXME what about partial heads?
  
  // write all dirty bufferheads
  bc.lock.Lock();

  dout(1) << "flush_all writing dirty bufferheads" << endl;
  while (!bc.dirty_bh.empty()) {
	set<BufferHead*>::iterator i = bc.dirty_bh.begin();
	BufferHead *bh = *i;
	if (bh->ioh) continue;
	Onode *on = get_onode(bh->oc->get_object_id());
	bh_write(on, bh);
	put_onode(on);
  }
  dout(1) << "flush_all submitted" << endl;

  
  while (bc.get_stat_tx() > 0 ||
		 bc.get_stat_partial() > 0) {
	dout(1) << "flush_all waiting for " << bc.get_stat_tx() << " tx, " << bc.get_stat_partial() << " partial" << endl;
	bc.waitfor_stat();
  }
  bc.lock.Unlock();
  dout(1) << "flush_all done" << endl;
}


// ? is this the best way ?
class C_E_FlushPartial : public Context {
  Ebofs *ebofs;
  Onode *on;
  BufferHead *bh;
public:
  C_E_FlushPartial(Ebofs *e, Onode *o, BufferHead *b) : ebofs(e), on(o), bh(b) {}
  void finish(int r) {
	if (r == 0) ebofs->bh_write(on, bh);
  }
};


void Ebofs::bh_read(Onode *on, BufferHead *bh)
{
  dout(5) << "bh_read " << *on << " on " << *bh << endl;
  assert(bh->get_version() == 0);
  assert(bh->is_rx() || bh->is_partial());
  
  // get extents
  vector<Extent> ex;
  on->map_extents(bh->start(), bh->length(), ex);
  
  // lay out on disk
  block_t ooff = 0;
  for (unsigned i=0; i<ex.size(); i++) {
	dout(10) << "bh_read  " << ooff << ": " << ex[i] << endl;
	bufferlist sub;
	sub.substr_of(bh->data, ooff*EBOFS_BLOCK_SIZE, ex[i].length*EBOFS_BLOCK_SIZE);

	if (bh->is_partial()) 
	  bh->waitfor_read.push_back(new C_E_FlushPartial(this, on, bh));

	assert(bh->ioh == 0);
	bh->ioh = dev.read(ex[i].start, ex[i].length, sub,
					   new C_OC_RxFinish(on->oc, ooff, ex[i].length));

	ooff += ex[i].length;
  }
}

void Ebofs::bh_write(Onode *on, BufferHead *bh)
{
  dout(5) << "bh_write " << *on << " on " << *bh << endl;
  assert(bh->get_version() > 0);

  assert(bh->is_dirty());
  bc.mark_tx(bh);

  // get extents
  vector<Extent> ex;
  on->map_extents(bh->start(), bh->length(), ex);

  // lay out on disk
  block_t bhoff = 0;
  for (unsigned i=0; i<ex.size(); i++) {
	dout(10) << "bh_write  bh off " << bhoff << ": " << ex[i] << endl;

	bufferlist sub;
	sub.substr_of(bh->data, bhoff*EBOFS_BLOCK_SIZE, ex[i].length*EBOFS_BLOCK_SIZE);

	assert(bh->ioh == 0);
	bh->ioh = dev.write(ex[i].start, ex[i].length, sub,
						new C_OC_TxFinish(on->oc, 
										  bhoff + bh->start(), ex[i].length, 
										  bh->get_version()));

	bhoff += ex[i].length;
  }
}



void Ebofs::apply_write(Onode *on, size_t len, off_t off, bufferlist& bl)
{
  ObjectCache *oc = on->get_oc(&bc);

  // map into blocks
  off_t opos = off;         // byte pos in object
  size_t zleft = 0;         // zeros left to write

  block_t bstart = off / EBOFS_BLOCK_SIZE;

  if (off > on->object_size) {
	zleft = off - on->object_size;
	opos = on->object_size;
	bstart = on->object_size / EBOFS_BLOCK_SIZE;
  }
  if (bl.length() == 0) 
	zleft += len;
  if (off+len > on->object_size) {
	dout(10) << "apply_write extending object size " << on->object_size 
			 << " -> " << off+len << endl;
	on->object_size = off+len;
  }
  if (zleft)
	dout(10) << "apply_write zeroing first " << zleft << " bytes" << endl;

  block_t blast = (len+off-1) / EBOFS_BLOCK_SIZE;
  block_t blen = blast-bstart+1;

  bc.lock.Lock();
    
  map<block_t, BufferHead*> hits;
  oc->map_write(bstart, blen, hits);

  // get current versions
  version_t lowv, highv;
  oc->scan_versions(bstart, blen, lowv, highv);
  
  // copy from bl into buffer cache
  unsigned blpos = 0;       // byte pos in input buffer
  size_t left = len;        // bytes left

  // write data into buffers
  for (map<block_t, BufferHead*>::iterator i = hits.begin();
	   i != hits.end(); 
	   i++) {
	BufferHead *bh = i->second;
	bh->set_version(highv+1);
	
	// cancel old io?
	if (bh->ioh) {
	  dout(10) << "apply_write canceling old io on " << *bh << endl;
	  bc.dev.cancel_io( bh->ioh );
	  bh->ioh = 0;
	}

	// partial at head or tail?
	if ((bh->start() == bstart && off % EBOFS_BLOCK_SIZE != 0) ||
		(bh->last() == blast && (len+off) % EBOFS_BLOCK_SIZE != 0)) {
	  // locate ourselves in bh
	  unsigned off_in_bh = opos - bh->start()*EBOFS_BLOCK_SIZE;
	  assert(off_in_bh >= 0);
	  unsigned len_in_bh = MIN( zleft+left,
								(off_t)(bh->end()*EBOFS_BLOCK_SIZE)-opos );
	  
	  if (bh->is_partial() || bh->is_rx() || bh->is_missing()) {
		assert(bh->is_partial() || bh->is_rx() || bh->is_missing());

		// add frag to partial
		dout(10) << "apply_write writing into partial " << *bh << ":"
				 << " off_in_bh " << off_in_bh 
				 << " len_in_bh " << len_in_bh
				 << endl;
		unsigned z = MIN( zleft, len_in_bh );
		if (z) {
		  bufferlist zb;
		  zb.push_back(new buffer(z));
		  zb.zero();
		  bh->add_partial(opos, zb);
 		  zleft -= z;
		  opos += z;
		}

		bufferlist p;
		p.substr_of(bl, blpos, len_in_bh-z);
		bh->add_partial(opos, p);
		left -= len_in_bh-z;
		blpos += len_in_bh-z;
		opos += len_in_bh-z;

		if (bh->partial_is_complete(on->object_size)) {
		  dout(10) << "apply_write  completed partial " << *bh << endl;
		  bc.bufferpool.alloc_list(bh->length(), bh->data);  // new buffers!
		  bh->data.zero();
		  bh->apply_partial();
		  bc.mark_dirty(bh);
		  if (bh->ioh) {
			bc.dev.cancel_io( bh->ioh );
			bh->ioh = 0;
		  }
		} 
		else if (bh->is_rx()) {
		  dout(10) << "apply_write  rx -> partial " << *bh << endl;
		  bc.mark_partial(bh);
		}
		else if (bh->is_missing()) {
		  dout(10) << "apply_write  missing -> partial " << *bh << endl;
		  bh_read(on, bh);	
		}
		else if (bh->is_partial()) {
		  if (bh->ioh == 0) {
			dout(10) << "apply_write  submitting rx for partial " << *bh << endl;
			bh_read(on, bh);	
		  }
		}
	  } else {
		assert(bh->is_clean() || bh->is_dirty() || bh->is_tx());
		
		// just write into the bh!
		dout(10) << "apply_write writing leading/tailing partial into " << *bh << ":"
				 << " off_in_bh " << off_in_bh 
				 << " len_in_bh " << len_in_bh
				 << endl;
		unsigned z = MIN( zleft, len_in_bh );
		if (z) {
		  bufferlist zb;
		  zb.push_back(new buffer(z));
		  bh->data.copy_in(off_in_bh, z, zb);
		  zleft -= z;
		  opos += z;
		}

		bufferlist sub;
		sub.substr_of(bl, blpos, len_in_bh-z);
		bh->data.copy_in(off_in_bh+z, len_in_bh-z, sub);
		blpos += len_in_bh-z;
		left -= len_in_bh-z;
		opos += len_in_bh-z;

		if (!bh->is_dirty()) 
		  bc.mark_dirty(bh);
	  }
	  continue;
	}

	// ok, we're talking full blocks now.


	// alloc new buffers.
	bc.bufferpool.alloc_list(len, bh->data);
	
	// copy!
	unsigned len_in_bh = bh->length()*EBOFS_BLOCK_SIZE;
	assert(len_in_bh >= zleft+left);
	
	dout(10) << "apply_write writing into " << *bh << ":"
			 << " len_in_bh " << len_in_bh
			 << endl;
	
	unsigned z = MIN(len_in_bh, zleft);
	if (z) {
	  bufferlist zb;
	  zb.push_back(new buffer(z));
	  zb.zero();
	  bh->data.copy_in(0, z, zb);
	  zleft -= z;
	}
	
	bufferlist sub;
	sub.substr_of(bl, blpos, len_in_bh-z);
	bh->data.copy_in(z, len_in_bh-z, sub);

	blpos += len_in_bh-z;
	left -= len_in_bh-z;
	opos += len_in_bh;

	// mark dirty
	if (!bh->is_dirty())
	  bc.mark_dirty(bh);
  }
  assert(zleft == 0);
  assert(left == 0);
  assert(opos == off+len);
  //assert(blpos == bl.length());

  bc.lock.Unlock();
}




// *** file i/o ***

int Ebofs::read(object_t oid, 
				size_t len, off_t off, 
				bufferlist& bl)
{
  
  return 0;
}


int Ebofs::write(object_t oid, 
				 size_t len, off_t off, 
				 bufferlist& bl, Context *onflush)
{
  assert(len > 0);
  
  // get inode
  Onode *on = get_onode(oid);
  if (!on) 
	on = new_onode(oid);	// new inode!

  // apply to buffer cache
  apply_write(on, len, off, bl);

  // allocate more space?
  block_t bnum = (len+off-1) / EBOFS_BLOCK_SIZE + 1;
  if (bnum > on->object_blocks) {
	block_t need = bnum - on->object_blocks;
	block_t near = 0;
	if (on->extents.size()) 
	  near = on->extents[on->extents.size()-1].end();
	
	while (need > 0) {
	  Extent ex;
	  allocator.allocate(ex, need, near);
	  dout(10) << "apply_write allocated " << ex << " near " << near << endl;
	  on->extents.push_back(ex);
	  on->object_blocks += ex.length;
	  need -= ex.length;
	  near = ex.end();
	}	
  }
  

  
  // attr changes
  // ***

  // set up onfinish waiter
  if (onflush) {
	
	
  }

  // done
  put_onode(on);

  return 0;
}
