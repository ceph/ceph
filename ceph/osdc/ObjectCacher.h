


class ObjectCacher {
  Objecter *objecter;


  class Object {
	
	class BufferHead {
	public:
	  const int CLEAN = 1;
	  const int DIRTY = 2;
	  const int RX = 3;
	  const int TX = 4;
	  int state;

	};

	map<size_t, BufferHead*> bh_map;

	class Lock {
	public:
	  const int NONE = 0;
	  const int WRLOCK = 1;
	  //const int RDLOCK = 2;
	  
	  int state;

	  Lock() : state(NONE) {}
	};

  };

  

  int map_read(OSDRead *rd);
  int map_write(OSDWrite *wr);


  void flush(set<object_t>& objects);  
  void flush_all();

  void commit(set<object_t>& objects);
  void commit_all();


  
};



// sync write (relaxed consistency, bypass cache)
Filer->prepare_write();
Objecter->writex(...);         // make async call
cond.Wait();                   // wait for ack

// sync write (correct)
Filer->prepare_write();
ObjectCache->atomic_sync_writex(...);  // blocks until sync write happens, or i get write locks

// async write
Filer->prepare_write();
ObjectCache->writex(...);      // non-blocking.  update cache.
 or
Objecter->writex(...);         // non-blocking.  don't update cache.
