// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
#include "bucket_cache.h"

using namespace file::listing;

bool Bucket::reclaim(const cohort::lru::ObjectFactory* newobj_fac) {
    auto factory = dynamic_cast<const Bucket::Factory*>(newobj_fac);
    if (factory == nullptr) {
        return false;
    }
#if 0
    /* make sure the reclaiming object is the same partiton with newobject factory,
        * then we can recycle the object, and replace with newobject */

    /* XXXX this upstream analysis has never made sense--we don't need the new
        * object to be in the same partition in order to remove the original object
        * from its original partition */
    if (! bc->cache.is_same_partition(name, factory->name)) [[unlikely]]  {
        return false;
    }
#endif
    {
      /* in this case, we are being called from a context which holds
       * A partition lock, and this may be still in use */
      lock_guard{mtx};
      if (! deleted()) {
	flags |= FLAG_DELETED;
	bc->recycle_count++;

	//std::cout << fmt::format("reclaim {}!", name) << std::endl;
	bc->un->remove_watch(name);

	/* XXX we MUST still be linked, so hook check is
	 * redundant--maybe it hopes (!) to compensate for "still in use" above */
#if 0
	if (name_hook.is_linked()) {
	  bc->cache.remove(hk, this, bucket_avl_cache::FLAG_NONE);
	}
#else
	bc->cache.remove(hk, this, bucket_avl_cache::FLAG_NONE);
#endif

	/* discard lmdb data associated with this bucket */
	auto txn = env->getRWTransaction();
	mdb_drop(*txn, dbi, 0); /* apparently, does not require commit */
      } /* ! deleted */
    }
    return true;
} /* reclaim */
