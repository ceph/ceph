/*
 * Startup executable for 
 * Ceph Active Storage. See README for details.
 *
 */
#include "activemaster.h"


/*
 * What main() must do:
 *
 *  - start up a Ceph client
 *  - find the set of OSDs that the file is striped across
 *  - start up the Map task on each OSD, using ssh
 *  - eat lunch?
 *  - start up the Reduce task locally
 */

int main(int argc, const char* argv[]) {  

  if (argc < 4) {
    usage(argv[0]);
    exit(-1);
  }

  const char* input_filename = argv[1];
  const char* map_command = argv[2];
  //const char* reduce_command = argv[3];

  // fire up the client
  Client* client = startCephClient();

  // open the file as read_only
  int fh = client->open(input_filename, O_RDONLY);
  if (fh < 0)    {
    cout << "The input file " << input_filename << " could not be opened." << endl;
    exit(-1);
  }

  // How big is the file?
  int filesize;
  struct stat stbuf;
  if (0 > client->lstat(input_filename, &stbuf))    {
    cout << "Error: could not retrieve size of input file " << input_filename << endl;
    exit(-1);
  }
  filesize = stbuf.st_size;
  if (filesize < 1) {
    cout << "Error: input file size is " << filesize << endl;
    exit(-1);
  }

  // retrieve all the object extents
  list<ObjectExtent> extents;
  int offset = 0;
  client->enumerate_layout(fh, extents, filesize, offset);
  
  // for each object extent, retrieve the OSD IP address and start up a Map task
  list<ObjectExtent>::iterator i;
  map<size_t, size_t>::iterator j;
  int osd;
  int start, length;
  tcpaddr_t tcpaddr;

  for (i = extents.begin(); i != extents.end(); i++)
    {
      // find the primary and get its IP address
      osd = client->osdmap->get_pg_primary(i->pgid);      
      entity_inst_t inst = client->osdmap->get_inst(osd); 
      entity_addr_t entity_addr = inst.addr;
      entity_addr.make_addr(tcpaddr);
      
      // iterate through each buffer_extent in the ObjectExtent
      for (j = i->buffer_extents.begin();
	   j != i->buffer_extents.end(); j++)
	{
	  // get the range of the buffer_extent
	  start = (*j).first;
	  length = (*j).second;
	  // fire up the Map task
	  start_map_task(map_command, input_filename, start, length, tcpaddr);
	}
    }
  return 0; 
}

// Fires up the map task.
// For the moment, all it does is echo the command line, not run it.
int start_map_task(const char* command, const char* input_filename,
		   long start, long length,  sockaddr_in ip_address)
{
  string ip_addr_string(inet_ntoa(ip_address.sin_addr));
  
  



  cout << "ssh " << ip_addr_string << " " << command 
       << " " << input_filename << " " << start << " " << length << endl;
  return 0;
}



void usage(const char* name) {
  cout << "usage: " << name << " inputfile map_task reduce_task" << endl;
  cout << "inputfile must be a valid path in the running Ceph filesystem." << endl;
  cout << "map_task should be given with an absolute path, and be present on ";
  cout << "the REGULAR filesystem every node." << endl;
  cout << "reduce_task need be present on this node only." << endl;
}


 

