
#include "cas.h"

object_t calc_cas_name(bufferlist &bl)
{


}


void find_rabin_markers(bufferlist &bl, uint64_t bloff, list<uint64_t> &chunks_offsets)
{
  
  
}


  tid_t write_cas_object(inodeno_t parent,
		  ceph_object_layout ol, 
		  bufferlist &bl, int flags,
		  Context *onack, Context *oncommit) {
    object_t name = calculate_cas_name(bl);
    while (1) {
      if (exists(name)) {
	int err = add_cas_ref(name, parent);
	if (does not exist)
	  continue;
	break;
      } else {
	write(name, bl, parent);
	break;
      }
    }
  }




min
  less than ideal
want
  ideal
max   worst case

