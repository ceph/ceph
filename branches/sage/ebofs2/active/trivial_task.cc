#include "trivial_task.h"

void start_trivial_task (const char* ceph_filename, const char* local_filename, 
			 off_t offset, off_t length) {
  // Don't bother to copy the file to disk. Read the file directly from Ceph,
  // and add up all the bytes.
  // Write the total to the local file as a string.
    Client * client = startCephClient();

    bufferptr bp(CHUNK);

    // get the source file's size. Sanity-check the request range.
    struct stat st;
    int r = client->lstat(ceph_filename, &st);
    assert (r == 0);
    
    off_t src_total_size = st.st_size;
    if (src_total_size < offset + length) {
      cerr << "Error in copy ExtentToLocalFile: offset + length = " << offset << " + " << length
	   << " = " + (offset + length) << ", source file size is only " << src_total_size << endl;
      exit(-1);
    }
    off_t remaining = length;
    
    // open the file and seek to the start position
    cerr << "start_trivial_task: opening the source file and seeking " << endl;

    int fh_ceph = client->open(ceph_filename, O_RDONLY);
    assert (fh_ceph > -1); 
    r = client->lseek(fh_ceph, offset, SEEK_SET);
    assert (r == offset);
    
    int counter = 0;
    // read through the extent and add up the bytes
    cerr << "start_trivial_task: counting up bytes" << endl;
    char* bp_c = bp.c_str();
    while (remaining > 0) {
      off_t got = client->read(fh_ceph, bp_c, MIN(remaining,CHUNK), -1);
      assert(got > 0);
      remaining -= got;
      for (off_t i = 0; i < got; ++i) {
	counter += (unsigned int)(bp_c[i]);
      }
    }
    cerr << "start_trivial_task: Done! Answer is " << counter << endl;
    client->close(fh_ceph);
        
    //assert(0);
}

