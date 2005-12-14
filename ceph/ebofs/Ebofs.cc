
#include "Ebofs.h"

// *******************

#undef dout
#define dout(x) if (x <= g_conf.debug) cout << "ebofs."

int Ebofs::mount()
{
  // note: this will fail in mount -> unmount -> mount type situations, bc
  //       prior state isn't fully cleaned up.

  dout(1) << "mount" << endl;

  ebofs_lock.Lock();
  assert(!mounted);

  // read super
  bufferptr bp1 = bufferpool.alloc(EBOFS_BLOCK_SIZE);
  bufferptr bp2 = bufferpool.alloc(EBOFS_BLOCK_SIZE);
  dev.read(0, 1, bp1);
  dev.read(1, 1, bp2);

  struct ebofs_super *sb1 = (struct ebofs_super*)bp1.c_str();
  struct ebofs_super *sb2 = (struct ebofs_super*)bp2.c_str();
  dout(3) << "mount super @0 epoch " << sb1->epoch << endl;
  dout(3) << "mount super @1 epoch " << sb2->epoch << endl;

  // pick newest super
  struct ebofs_super *sb = 0;
  if (sb1->epoch > sb2->epoch)
	sb = sb1;
  else
	sb = sb2;
  super_epoch = sb->epoch;
  dout(3) << "mount epoch " << super_epoch << endl;
  assert(super_epoch == sb->epoch);

  // init node pools
  dout(3) << "mount nodepool" << endl;
  nodepool.init( &sb->nodepool );
  nodepool.read_usemap( dev, super_epoch );
  nodepool.read_clean_nodes( dev );
  
  // open tables
  dout(3) << "mount opening tables" << endl;
  object_tab = new Table<object_t, Extent>( nodepool, sb->object_tab );
  for (int i=0; i<EBOFS_NUM_FREE_BUCKETS; i++)
	free_tab[i] = new Table<block_t, block_t>( nodepool, sb->free_tab[i] );
  
  collection_tab = new Table<coll_t, Extent>( nodepool, sb->collection_tab );
  oc_tab = new Table<idpair_t, bool>( nodepool, sb->oc_tab );
  co_tab = new Table<idpair_t, bool>( nodepool, sb->co_tab );
  
  dout(3) << "mount starting commit thread" << endl;
  commit_thread.create();
  
  dout(1) << "mount mounted" << endl;
  mounted = true;

  ebofs_lock.Unlock();
  return 0;
}


int Ebofs::mkfs()
{
  ebofs_lock.Lock();
  assert(!mounted);

  block_t num_blocks = dev.get_num_blocks();

  // create first noderegion
  Extent nr;
  nr.start = 2;
  nr.length = num_blocks / 10000;
  if (nr.length < 10) nr.length = 10;
  nodepool.add_region(nr);
  dout(1) << "mkfs: first node region at " << nr << endl;

  // allocate two usemaps
  block_t usemap_len = ((nr.length-1) / 8 / EBOFS_BLOCK_SIZE) + 1;
  nodepool.usemap_even.start = nr.end();
  nodepool.usemap_even.length = usemap_len;
  nodepool.usemap_odd.start = nodepool.usemap_even.end();
  nodepool.usemap_odd.length = usemap_len;
  dout(1) << "mkfs: even usemap at " << nodepool.usemap_even << endl;
  dout(1) << "mkfs:  odd usemap at " << nodepool.usemap_odd << endl;
  
  // init tables
  struct ebofs_table empty;
  empty.num_keys = 0;
  empty.root = -1;
  empty.depth = 0;
  
  object_tab = new Table<object_t, Extent>( nodepool, empty );
  collection_tab = new Table<coll_t, Extent>( nodepool, empty );
  
  for (int i=0; i<EBOFS_NUM_FREE_BUCKETS; i++)
	free_tab[i] = new Table<block_t,block_t>( nodepool, empty );
  
  oc_tab = new Table<idpair_t, bool>( nodepool, empty );
  co_tab = new Table<idpair_t, bool>( nodepool, empty );

  // add free space
  Extent left;
  left.start = nodepool.usemap_odd.end();
  left.length = num_blocks - left.start;
  dout(1) << "mkfs: free blocks at " << left << endl;
  allocator.release_now( left );

  // write nodes, super, 2x
  dout(1) << "mkfs: flushing nodepool and superblocks (2x)" << endl;

  nodepool.commit_start( dev, 0 );
  nodepool.commit_wait();
  bufferptr superbp0;
  prepare_super(0, superbp0);
  write_super(0, superbp0);
  
  nodepool.commit_start( dev, 1 );
  nodepool.commit_wait();
  bufferptr superbp1;
  prepare_super(1, superbp1);
  write_super(1, superbp1);
  
  // free memory
  dout(3) << "mkfs: cleaning up" << endl;
  close_tables();

  dout(1) << "mkfs: done" << endl;

  ebofs_lock.Unlock();
  return 0;
}

void Ebofs::close_tables() 
{
  // close tables
  delete object_tab;
  for (int i=0; i<EBOFS_NUM_FREE_BUCKETS; i++)
	delete free_tab[i];
  delete collection_tab;
  delete oc_tab;
  delete co_tab;
}

int Ebofs::umount()
{
  ebofs_lock.Lock();
  
  // mark unmounting
  dout(1) << "umount start" << endl;
  readonly = true;
  unmounting = true;
  
  // kick commit thread
  commit_cond.Signal();

  // wait 
  dout(2) << "umount stopping commit thread" << endl;
  ebofs_lock.Unlock();
  commit_thread.join();
  ebofs_lock.Lock();

  // free memory
  dout(2) << "umount cleaning up" << endl;
  close_tables();

  dout(1) << "umount done" << endl;
  ebofs_lock.Unlock();
  return 0;
}



void Ebofs::prepare_super(version_t epoch, bufferptr& bp)
{
  struct ebofs_super sb;
  
  dout(10) << "prepare_super v" << epoch << endl;

  // fill in super
  memset(&sb, 0, sizeof(sb));
  sb.s_magic = EBOFS_MAGIC;
  sb.epoch = epoch;
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

  sb.collection_tab.num_keys = collection_tab->get_num_keys();
  sb.collection_tab.root = collection_tab->get_root();
  sb.collection_tab.depth = collection_tab->get_depth();

  sb.oc_tab.num_keys = oc_tab->get_num_keys();
  sb.oc_tab.root = oc_tab->get_root();
  sb.oc_tab.depth = oc_tab->get_depth();

  sb.co_tab.num_keys = co_tab->get_num_keys();
  sb.co_tab.root = co_tab->get_root();
  sb.co_tab.depth = co_tab->get_depth();

  // pools
  sb.nodepool.num_regions = nodepool.region_loc.size();
  for (unsigned i=0; i<nodepool.region_loc.size(); i++) {
	sb.nodepool.region_loc[i] = nodepool.region_loc[i];
  }
  sb.nodepool.node_usemap_even = nodepool.usemap_even;
  sb.nodepool.node_usemap_odd = nodepool.usemap_odd;
  
  // put in a buffer
  bp = bufferpool.alloc(EBOFS_BLOCK_SIZE);
  memcpy(bp.c_str(), (const char*)&sb, sizeof(sb));
}

void Ebofs::write_super(version_t epoch, bufferptr& bp)
{
  block_t bno = epoch & 1;
  
  dout(10) << "write_super v" << epoch << " to b" << bno << endl;

  dev.write(bno, 1, bp);
}

int Ebofs::commit_thread_entry()
{  
  ebofs_lock.Lock();
  dout(10) << "commit_thread start" << endl;

  commit_thread_started = true;
  sync_cond.Signal();

  while (mounted) {
	
	// wait
	//commit_cond.Wait(ebofs_lock, utime_t(EBOFS_COMMIT_INTERVAL,0));   // wait for kick, or 10s.
	commit_cond.Wait(ebofs_lock);//, utime_t(EBOFS_COMMIT_INTERVAL,0));   // wait for kick, or 10s.

	if (unmounting) {
	  dout(10) << "commit_thread unmounting: final commit pass" << endl;
	  assert(readonly);
	  unmounting = false;
	  mounted = false;
	}

	super_epoch++;
	dout(10) << "commit_thread commit start, new epoch is " << super_epoch << endl;

	// (async) write onodes+condes  (do this first; it currently involves inode reallocation)
	commit_inodes_start();
	
	allocator.release_limbo();

	// (async) write btree nodes
	nodepool.commit_start( dev, super_epoch );
	
	// prepare super (before any changes get made!)
	bufferptr superbp;
	prepare_super(super_epoch, superbp);

	// wait it all to flush (drops global lock)
	commit_inodes_wait();
	nodepool.commit_wait();
	commit_bc_wait(super_epoch-1);
	
	// ok, now (synchronously) write the prior super!
	dout(10) << "commit_thread commit flushed, writing super for prior epoch" << endl;
	ebofs_lock.Unlock();
	write_super(super_epoch, superbp);	
	ebofs_lock.Lock();

	sync_cond.Signal();

	dout(10) << "commit_thread commit finish" << endl;
  }
  
  dout(10) << "commit_thread finish" << endl;
  ebofs_lock.Unlock();
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
  bufferpool.alloc( EBOFS_BLOCK_SIZE*onode_loc.length, bl );
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
	on->attr[key] = AttrVal(p, len);
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


void Ebofs::write_onode(Onode *on, Context *c)
{
  // buffer
  int bytes = sizeof(ebofs_onode) + on->get_attr_bytes() + on->get_extent_bytes();
  unsigned blocks = (bytes-1)/EBOFS_BLOCK_SIZE + 1;

  bufferlist bl;
  bufferpool.alloc( EBOFS_BLOCK_SIZE*blocks, bl );

  // (always) relocate onode
  if (1) {
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
  for (map<string, AttrVal>::iterator i = on->attr.begin();
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
  dev.write( on->onode_loc.start, on->onode_loc.length, bl, c );
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

void Ebofs::dirty_onode(Onode *on)
{
  if (!on->is_dirty()) {
	on->mark_dirty();
	dirty_onodes.insert(on);
  }
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



// *** cnodes ****

Cnode* Ebofs::new_cnode(object_t cid)
{
  Cnode* cn = new Cnode(cid);

  assert(cnode_map.count(cid) == 0);
  cnode_map[cid] = cn;
  cnode_lru.lru_insert_mid(cn);
  
  collection_tab->insert( cid, cn->cnode_loc );  // even tho i'm not placed yet
  
  cn->get();
  return cn;
}

Cnode* Ebofs::get_cnode(object_t cid)
{
  // in cache?
  if (cnode_map.count(cid)) {
	// yay
	Cnode *cn = cnode_map[cid];
	cn->get();
	return cn;   
  }

  // on disk?
  Extent cnode_loc;
  if (collection_tab->lookup(cid, cnode_loc) != Table<coll_t,Extent>::Cursor::MATCH) {
	// object dne.
	return 0;
  }

  // read it!
  bufferlist bl;
  bufferpool.alloc( EBOFS_BLOCK_SIZE*cnode_loc.length, bl );
  dev.read( cnode_loc.start, cnode_loc.length, bl );

  // parse data block
  Cnode *cn = new Cnode(cid);

  struct ebofs_cnode *ec = (struct ebofs_cnode*)bl.c_str();
  cn->cnode_loc = ec->cnode_loc;

  // parse attributes
  char *p = bl.c_str() + sizeof(*ec);
  for (int i=0; i<ec->num_attr; i++) {
	string key = p;
	p += key.length() + 1;
	int len = *(int*)(p);
	p += sizeof(len);
	cn->attr[key] = AttrVal(p, len);
  }

  cn->get();
  return cn;
}

void Ebofs::write_cnode(Cnode *cn, Context *c)
{
  // allocate buffer
  int bytes = sizeof(ebofs_cnode) + cn->get_attr_bytes();
  unsigned blocks = (bytes-1)/EBOFS_BLOCK_SIZE + 1;
  
  bufferlist bl;
  bufferpool.alloc( EBOFS_BLOCK_SIZE*blocks, bl );

  // (always) relocate cnode!
  if (1) {
	if (cn->cnode_loc.length) 
	  allocator.release(cn->cnode_loc);
	
	allocator.allocate(cn->cnode_loc, blocks, 0);
	collection_tab->remove( cn->coll_id );
	collection_tab->insert( cn->coll_id, cn->cnode_loc );
  }
  
  struct ebofs_cnode ec;
  ec.cnode_loc = cn->cnode_loc;
  ec.coll_id = cn->coll_id;
  ec.num_attr = cn->attr.size();
  
  bl.copy_in(0, sizeof(ec), (char*)&ec);
  
  // attr
  unsigned off = sizeof(ec);
  for (map<string, AttrVal >::iterator i = cn->attr.begin();
	   i != cn->attr.end();
	   i++) {
	bl.copy_in(off, i->first.length()+1, i->first.c_str());
	off += i->first.length()+1;
	bl.copy_in(off, sizeof(int), (char*)&i->second.len);
	off += sizeof(int);
	bl.copy_in(off, i->second.len, i->second.data);
	off += i->second.len;
  }
  
  // write
  dev.write( cn->cnode_loc.start, cn->cnode_loc.length, bl, c );
}

void Ebofs::remove_cnode(Cnode *cn)
{
  // remove from table
  collection_tab->remove(cn->coll_id);

  // free cnode space
  if (cn->cnode_loc.length)
	allocator.release(cn->cnode_loc);

  // delete mappings
  //cn->clear();

  delete cn;
}

void Ebofs::put_cnode(Cnode *cn)
{
  cn->put();
}

void Ebofs::dirty_cnode(Cnode *cn)
{
  if (!cn->is_dirty()) {
	cn->mark_dirty();
	dirty_cnodes.insert(cn);
  }
}




class C_E_InodeFlush : public Context {
  Ebofs *ebofs;
public:
  C_E_InodeFlush(Ebofs *e) : ebofs(e) {}
  void finish(int r) {
	ebofs->flush_inode_finish();
  }
};

void Ebofs::flush_inode_finish()
{
  ebofs_lock.Lock();
  inodes_flushing--;
  if (inodes_flushing == 0) 
	inode_commit_cond.Signal();
  ebofs_lock.Unlock();
}

void Ebofs::commit_inodes_start() 
{
  dout(10) << "commit_inodes_start" << endl;

  assert(inodes_flushing == 0);

  // onodes
  for (set<Onode*>::iterator i = dirty_onodes.begin();
	   i != dirty_onodes.end();
	   i++) {
	Onode *on = *i;
	inodes_flushing++;
	write_onode(on, new C_E_InodeFlush(this));
	on->mark_clean();
	on->uncommitted.clear();   // commit allocated blocks
  }
  dirty_onodes.clear();

  // cnodes
  for (set<Cnode*>::iterator i = dirty_cnodes.begin();
	   i != dirty_cnodes.end();
	   i++) {
	Cnode *cn = *i;
	inodes_flushing++;
	write_cnode(cn, new C_E_InodeFlush(this));
	cn->mark_clean();
  }
  dirty_cnodes.clear();

  dout(10) << "commit_inodes_start writing " << inodes_flushing << " onodes+cnodes" << endl;
}

void Ebofs::commit_inodes_wait()
{
  // caller must hold ebofs_lock
  while (inodes_flushing > 0) {
	dout(10) << "commit_inodes_wait waiting for " << inodes_flushing << " onodes+cnodes to flush" << endl;
	inode_commit_cond.Wait(ebofs_lock);
  }
  dout(10) << "commit_inodes_wait all flushed" << endl;
}







// *** buffer cache ***

void Ebofs::trim_buffer_cache()
{
  ebofs_lock.Lock();
  dout(10) << "trim_buffer_cache start: " 
		   << bc.lru_rest.lru_get_size() << " rest + " 
		   << bc.lru_dirty.lru_get_size() << " dirty " << endl;

  // trim trimmable bufferheads
  while (bc.lru_rest.lru_get_size() > bc.lru_rest.lru_get_max()) {
	BufferHead *bh = (BufferHead*) bc.lru_rest.lru_expire();
	if (!bh) break;
	
	dout(10) << "trim_buffer_cache trimming " << *bh << endl;
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
  dout(10) << "trim_buffer_cache finish: " 
		   << bc.lru_rest.lru_get_size() << " rest + " 
		   << bc.lru_dirty.lru_get_size() << " dirty " << endl;
  
  ebofs_lock.Unlock();
}


void Ebofs::sync()
{
  ebofs_lock.Lock();
  dout(3) << "sync in " << super_epoch << endl;
  
  if (!commit_thread_started) {
	dout(10) << "sync waiting for commit thread to start" << endl;
	sync_cond.Wait(ebofs_lock);
  }

  if (mid_commit) {
	dout(10) << "sync waiting for commit in progress" << endl;
	sync_cond.Wait(ebofs_lock);
  }
  
  commit_cond.Signal();  // trigger a commit
  
  sync_cond.Wait(ebofs_lock);  // wait

  dout(3) << "sync finish in " << super_epoch << endl;
  ebofs_lock.Unlock();
}


void Ebofs::commit_bc_wait(version_t epoch)
{
  dout(10) << "commit_bc_wait on epoch " << epoch << endl;  
  
  while (bc.get_unflushed(epoch) > 0) {
	dout(10) << "commit_bc_wait " << bc.get_unflushed(epoch) << " unflushed in epoch " << epoch << endl;
	bc.waitfor_stat();
  }

  dout(10) << "commit_bc_wait all flushed for epoch " << epoch << endl;  
}






/*
 * allocate a write to blocks on disk.
 * - take care to not overwrite any "safe" data blocks.
 *  - allocate/map new extents on disk as necessary
 */
void Ebofs::alloc_write(Onode *on, 
						block_t start, block_t len,
						interval_set<block_t>& alloc)
{
  // first decide what pages to (re)allocate
  on->map_alloc_regions(start, len, alloc);

  dout(10) << "alloc_write need to (re)alloc " << alloc << endl;

  // merge alloc into onode uncommitted map
  //dout(10) << " union of " << on->uncommitted << " and " << alloc << endl;
  on->uncommitted.union_of(alloc);
  dout(10) << "alloc_write onode.uncommitted is now " << on->uncommitted << endl;

  // allocate the space
  for (map<block_t,block_t>::iterator i = alloc.m.begin();
	   i != alloc.m.end();
	   i++) {
	// get old region
	vector<Extent> old;
	on->map_extents(i->first, i->second, old);
	for (unsigned o=0; o<old.size(); o++)
	  allocator.release(old[o]);
	
	// allocate new space
	block_t left = i->second;
	block_t cur = i->first;
	while (left > 0) {
	  Extent ex;
	  allocator.allocate(ex, left, 0);
	  dout(10) << "alloc_write got " << ex << " for object offset " << cur << endl;
	  on->set_extent(cur, ex);      // map object to new region
	  left -= ex.length;
	  cur += ex.length;
	}
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
	dirty_onode(on);
  }
  if (zleft)
	dout(10) << "apply_write zeroing first " << zleft << " bytes" << endl;

  block_t blast = (len+off-1) / EBOFS_BLOCK_SIZE;
  block_t blen = blast-bstart+1;

  // allocate write on disk.
  interval_set<block_t> alloc;
  alloc_write(on, bstart, blen, alloc);

  // map b range onto buffer_heads
  map<block_t, BufferHead*> hits;
  oc->map_write(on, bstart, blen, alloc, hits);
  
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
	bh->epoch_modified = super_epoch;
	
	// old write in progress?
	if (bh->is_tx()) {	  // copy the buffer to avoid munging up in-flight write
	  dout(10) << "apply_write tx pending, copying buffer on " << *bh << endl;
	  bufferlist temp;
	  temp.claim(bh->data);
	  bc.bufferpool.alloc(EBOFS_BLOCK_SIZE*bh->length(), bh->data); 
	  bh->data.copy_in(0, bh->length()*EBOFS_BLOCK_SIZE, temp);
	}

	// need to split off partial?
	if (bh->is_missing() && bh->length() > 1 &&
		(bh->start() == bstart && off % EBOFS_BLOCK_SIZE != 0)) {
	  BufferHead *right = bc.split(bh, bh->start()+1);
	  hits[right->start()] = right;
	  dout(10) << "apply_write split off left block for partial write; rest is " << *right << endl;
	}
	if (bh->is_missing() && bh->length() > 1 &&
		(bh->last() == blast && len+off % EBOFS_BLOCK_SIZE != 0) &&
		(len+off < on->object_size)) {
	  BufferHead *right = bc.split(bh, bh->last());
	  hits[right->start()] = right;
	  dout(10) << "apply_write split off right block for upcoming partial write; rest is " << *right << endl;
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
		  bc.bufferpool.alloc(EBOFS_BLOCK_SIZE*bh->length(), bh->data);  // new buffers!
		  bh->data.zero();
		  bh->apply_partial();
		  bc.mark_dirty(bh);
		  bc.bh_write(on, bh);
		} 
		else if (bh->is_rx()) {
		  dout(10) << "apply_write  rx -> partial " << *bh << endl;
		  assert(bh->length() == 1);
		  bc.mark_partial(bh);
		  bc.bh_queue_partial_write(on, bh);		  // queue the eventual write
		}
		else if (bh->is_missing()) {
		  dout(10) << "apply_write  missing -> partial " << *bh << endl;
		  assert(bh->length() == 1);
		  bc.mark_partial(bh);
		  bc.bh_read(on, bh);	
		  bc.bh_queue_partial_write(on, bh);		  // queue the eventual write
		}
		else if (bh->is_partial()) {
		  dout(10) << "apply_write  already partial, no need to submit rx on " << *bh << endl;
		  bc.bh_queue_partial_write(on, bh);		  // queue the eventual write
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
		  zb.zero();
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

		bc.bh_write(on, bh);
	  }
	  continue;
	}

	// ok, we're talking full block(s) now.
	assert(opos % EBOFS_BLOCK_SIZE == 0);
	assert(zleft+left >= (off_t)(EBOFS_BLOCK_SIZE*bh->length()));

	// alloc new buffers.
	bc.bufferpool.alloc(EBOFS_BLOCK_SIZE*bh->length(), bh->data);
	
	// copy!
	unsigned len_in_bh = bh->length()*EBOFS_BLOCK_SIZE;
	assert(len_in_bh <= zleft+left);
	
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

	bc.bh_write(on, bh);
  }

  assert(zleft == 0);
  assert(left == 0);
  assert(opos == off+len);
  //assert(blpos == bl.length());
}




// *** file i/o ***

class C_E_Cond : public Context {
  Cond *cond;
public:
  C_E_Cond(Cond *c) : cond(c) {}
  void finish(int r) {
	cond->Signal();
  }
};

bool Ebofs::attempt_read(Onode *on, size_t len, off_t off, bufferlist& bl, Cond *will_wait_on)
{
  dout(10) << "attempt_read " << *on << " len " << len << " off " << off << endl;
  ObjectCache *oc = on->get_oc(&bc);

  // map
  block_t bstart = off / EBOFS_BLOCK_SIZE;
  block_t blast = (len+off-1) / EBOFS_BLOCK_SIZE;
  block_t blen = blast-bstart+1;

  map<block_t, BufferHead*> hits;
  map<block_t, BufferHead*> missing;  // read these
  map<block_t, BufferHead*> rx;       // wait for these
  map<block_t, BufferHead*> partials;  // ??
  oc->map_read(on, bstart, blen, hits, missing, rx, partials);

  // missing buffers?
  if (!missing.empty()) {
	for (map<block_t,BufferHead*>::iterator i = missing.begin();
		 i != missing.end();
		 i++) {
	  dout(15) <<"attempt_read missing buffer " << *(i->second) << endl;
	  bc.bh_read(on, i->second);
	}
	BufferHead *wait_on = missing.begin()->second;
	wait_on->waitfor_read.push_back(new C_E_Cond(will_wait_on));
	return false;
  }
  
  // are partials sufficient?
  bool partials_ok = true;
  for (map<block_t,BufferHead*>::iterator i = partials.begin();
	   i != partials.end();
	   i++) {
	off_t start = MAX( off, (off_t)(i->second->start()*EBOFS_BLOCK_SIZE) );
	off_t end = MIN( off+len, (off_t)(i->second->end()*EBOFS_BLOCK_SIZE) );
	
	if (!i->second->have_partial_range(start, end)) {
	  if (partials_ok) {
		// wait on this one
		dout(15) <<"attempt_read insufficient partial buffer " << *(i->second) << endl;
		i->second->waitfor_read.push_back(new C_E_Cond(will_wait_on));
	  }
	  partials_ok = false;
	}
  }
  if (!partials_ok) return false;

  // wait on rx?
  if (!rx.empty()) {
	BufferHead *wait_on = rx.begin()->second;
	dout(15) <<"attempt_read waiting for read to finish on " << *wait_on << endl;
	wait_on->waitfor_read.push_back(new C_E_Cond(will_wait_on));
	return false;
  }

  // yay, we have it all!
  // concurrently walk thru hits, partials.
  map<block_t,BufferHead*>::iterator h = hits.begin();
  map<block_t,BufferHead*>::iterator p = partials.begin();

  off_t pos = off;
  block_t curblock = bstart;
  while (curblock <= blast) {
	BufferHead *bh = 0;
	if (h->first == curblock) {
	  bh = h->second;
	  h++;
	} else if (p->first == curblock) {
	  bh = p->second;
	  p++;
	} else assert(0);
	
	off_t bhstart = (off_t)(bh->start()*EBOFS_BLOCK_SIZE);
	off_t bhend = (off_t)(bh->end()*EBOFS_BLOCK_SIZE);
	off_t start = MAX( pos, bhstart );
	off_t end = MIN( off+len, bhend );
	
	if (bh->is_partial()) {
	  // copy from a partial block.  yuck!
	  bufferlist frag;
	  bh->copy_partial_substr( start, end, frag );
	  bl.claim_append( frag );
	  pos += frag.length();
	} else {
	  // copy from a full block.
	  if (bhstart == start && bhend == end) {
		bl.append( bh->data );
		pos += bh->data.length();
	  } else {
		bufferlist frag;
		frag.substr_of(bh->data, start-bhstart, end-start);
		pos += frag.length();
		bl.claim_append( frag );
	  }
	}

	curblock = bh->end();
	assert((off_t)(curblock*EBOFS_BLOCK_SIZE) == pos ||
		   end != bhend);
  }

  assert(bl.length() == len);
  return true;
}

int Ebofs::read(object_t oid, 
				size_t len, off_t off, 
				bufferlist& bl)
{
  ebofs_lock.Lock();
  dout(7) << "read " << hex << oid << dec << " len " << len << " off " << off << endl;

  Onode *on = get_onode(oid);
  if (!on) {
	ebofs_lock.Unlock();
	return -1;  // object dne?
  }

  // read data into bl.  block as necessary.
  Cond cond;

  while (1) {
	// check size bound
	if (off >= on->object_size) 
	  break;

	size_t will_read = MIN(off+len, on->object_size) - off;
	
	if (attempt_read(on, will_read, off, bl, &cond))
	  break;  // yay
	
	// wait
	cond.Wait(ebofs_lock);
  }

  put_onode(on);

  ebofs_lock.Unlock();
  return 0;
}


int Ebofs::write(object_t oid, 
				 size_t len, off_t off, 
				 bufferlist& bl, bool fsync)
{
  // wait?
  if (fsync) {
	// wait for flush.

	// FIXME.  wait on a Cond or whatever!  be careful about ebofs_lock.

	return write(oid, len, off, bl, (Context*)0);
  } else {
	// don't wait.
	return write(oid, len, off, bl, (Context*)0);
  }
}

int Ebofs::write(object_t oid, 
				 size_t len, off_t off, 
				 bufferlist& bl, Context *onflush)
{
  ebofs_lock.Lock();
  dout(7) << "write " << hex << oid << dec << " len " << len << " off " << off << endl;
  assert(len > 0);
  
  // get inode
  Onode *on = get_onode(oid);
  if (!on) 
	on = new_onode(oid);	// new inode!
  
  // apply write to buffer cache
  apply_write(on, len, off, bl);

  // apply attribute changes
  // ***

  // prepare (eventual) journal entry.

  // set up onfinish waiter
  if (onflush) {
	
  }

  // done
  put_onode(on);

  ebofs_lock.Unlock();
  return 0;
}


int Ebofs::remove(object_t oid)
{
  ebofs_lock.Lock();
  dout(7) << "remove " << hex << oid << dec << endl;
  
  // get inode
  Onode *on = get_onode(oid);
  if (!on) {
	ebofs_lock.Unlock();
	return -1;
  }

  // FIXME locking, buffer, flushing etc.
  assert(0);

  remove_onode(on);  

  ebofs_lock.Unlock();
  return 0;
}

int Ebofs::truncate(object_t oid, off_t size)
{
  assert(0);
}



bool Ebofs::exists(object_t oid)
{
  ebofs_lock.Lock();
  Onode *on = get_onode(oid);
  if (on) put_onode(on);
  ebofs_lock.Unlock();
  return on ? true:false;
}

int Ebofs::stat(object_t oid, struct stat *st)
{
  ebofs_lock.Lock();
  
  Onode *on = get_onode(oid);
  if (!on) {
	ebofs_lock.Unlock();
	return -1;
  }
  
  // ??
  st->st_size = on->object_size;

  put_onode(on);
  ebofs_lock.Unlock();
  return 0;
}

// attributes

int Ebofs::setattr(object_t oid, const char *name, void *value, size_t size)
{
  Onode *on = get_onode(oid);
  if (!on) return -1;

  string n(name);
  AttrVal val((char*)value, size);
  on->attr[n] = val;
  dirty_onode(on);

  put_onode(on);
  return 0;
}

int Ebofs::getattr(object_t oid, const char *name, void *value, size_t size)
{
  Onode *on = get_onode(oid);
  if (!on) return -1;

  string n(name);
  if (on->attr.count(n) == 0) return -1;
  memcpy(value, on->attr[n].data, MIN( on->attr[n].len, (int)size ));

  dirty_onode(on);
  put_onode(on);
  return 0;
}

int Ebofs::rmattr(object_t oid, const char *name) 
{
  Onode *on = get_onode(oid);
  if (!on) return -1;

  string n(name);
  on->attr.erase(n);

  dirty_onode(on);
  put_onode(on);
  return 0;
}

int Ebofs::listattr(object_t oid, vector<string>& attrs)
{
  Onode *on = get_onode(oid);
  if (!on) return -1;

  attrs.clear();
  for (map<string,AttrVal>::iterator i = on->attr.begin();
	   i != on->attr.end();
	   i++) {
	attrs.push_back(i->first);
  }

  put_onode(on);
  return 0;
}



/***************** collections ******************/

int Ebofs::list_collections(list<coll_t>& ls)
{
  Table<coll_t, Extent>::Cursor cursor(collection_tab);

  int num = 0;
  if (collection_tab->find(0, cursor) >= 0) {
	while (1) {
	  ls.push_back(cursor.current().key);
	  num++;
	  if (cursor.move_right() < 0) break;
	}
  }

  return num;
}

int Ebofs::create_collection(coll_t cid)
{
  if (collection_exists(cid)) return -1;
  Cnode *cn = new_cnode(cid);
  put_cnode(cn);
  return 0;
}

int Ebofs::destroy_collection(coll_t cid)
{
  if (!collection_exists(cid)) return -1;
  Cnode *cn = new_cnode(cid);
  
  // hose mappings
  list<object_t> objects;
  collection_list(cid, objects);
  for (list<object_t>::iterator i = objects.begin(); 
	   i != objects.end();
	   i++) {
	oc_tab->remove(idpair_t(*i,cid));
	co_tab->remove(idpair_t(cid,*i));
  }

  remove_cnode(cn);
  return 0;
}

bool Ebofs::collection_exists(coll_t cid)
{
  Table<coll_t, Extent>::Cursor cursor(collection_tab);
  if (collection_tab->find(cid, cursor) == Table<coll_t, Extent>::Cursor::MATCH) 
	return true;
  return false;
}

int Ebofs::collection_add(coll_t cid, object_t oid)
{
  if (!collection_exists(cid)) return -1;
  oc_tab->insert(idpair_t(oid,cid), true);
  co_tab->insert(idpair_t(cid,oid), true);
  return 0;
}

int Ebofs::collection_remove(coll_t cid, object_t oid)
{
  if (!collection_exists(cid)) return -1;
  oc_tab->remove(idpair_t(oid,cid));
  co_tab->remove(idpair_t(cid,oid));
  return 0;
}

int Ebofs::collection_list(coll_t cid, list<object_t>& ls)
{
  if (!collection_exists(cid)) return -1;
  
  Table<idpair_t, bool>::Cursor cursor(co_tab);

  int num = 0;
  if (co_tab->find(idpair_t(cid,0), cursor) >= 0) {
	while (1) {
	  const coll_t c = cursor.current().key.first;
	  const object_t o = cursor.current().key.second;
	  if (c != cid) break;   // end!
	  ls.push_back(o);
	  num++;
	  if (cursor.move_right() < 0) break;
	}
  }

  return num;
}


int Ebofs::collection_setattr(coll_t cid, const char *name, void *value, size_t size)
{
  Cnode *cn = get_cnode(cid);
  if (!cn) return -1;

  string n(name);
  AttrVal val((char*)value, size);
  cn->attr[n] = val;
  dirty_cnode(cn);

  put_cnode(cn);
  return 0;
}

int Ebofs::collection_getattr(coll_t cid, const char *name, void *value, size_t size)
{
  Cnode *cn = get_cnode(cid);
  if (!cn) return -1;

  string n(name);
  if (cn->attr.count(n) == 0) return -1;
  memcpy(value, cn->attr[n].data, MIN( cn->attr[n].len, (int)size ));

  put_cnode(cn);
  return 0;
}

int Ebofs::collection_rmattr(coll_t cid, const char *name) 
{
  Cnode *cn = get_cnode(cid);
  if (!cn) return -1;

  string n(name);
  cn->attr.erase(n);

  dirty_cnode(cn);
  put_cnode(cn);
  return 0;
}

int Ebofs::collection_listattr(coll_t cid, vector<string>& attrs)
{
  Cnode *cn = get_cnode(cid);
  if (!cn) return -1;

  attrs.clear();
  for (map<string,AttrVal>::iterator i = cn->attr.begin();
	   i != cn->attr.end();
	   i++) {
	attrs.push_back(i->first);
  }

  put_cnode(cn);
  return 0;
}


