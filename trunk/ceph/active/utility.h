/*
 * Miscellaneous Active OSD helper functions.
 *
 */

//#include <sys/stat.h>
#include "client/Client.h"
#include "common.h"
#include "config.h"
#include "common/Timer.h"
#include "msg/SimpleMessenger.h"
#include "socket_utility.h"

Client* startCephClient();
void kill_client(Client* client);

int send_msg_header(int fd, int header_ID);
int readmsgtype(int fd);
bool check_footer(int fd);
int send_msg_header(int fd, int header_ID);
int send_msg_footer(int fd);

/*
 * Fires up a Ceph client and returns a pointer to it.
 */ 

Client* startCephClient()
{
  cout << "ActiveMaster: Initializing Ceph client:" << endl;
  
  // parse args from CEPH_ARGS, not command line 
  vector<char*> args; 
  env_to_vec(args);
  parse_config_options(args);

  if (g_conf.clock_tare) g_clock.tare();

  // be safe
  g_conf.use_abspaths = true;

  // load monmap
  MonMap* monmap = new MonMap();
  int r = monmap->read(".ceph_monmap");
  if (r < 0) {
    cout << "ActiveMaster: could not find .ceph_monmap" << endl; 
    return 0;
  }
  assert(r >= 0);

  // start up network
  rank.start_rank();

  // start client
  Client *client;
  client = new Client(rank.register_entity(MSG_ADDR_CLIENT_NEW), monmap);
  client->init();
    
  // mount
  client->mount();
   
  return client;
}

void kill_client (Client * client)
{ 
  client->unmount();
  client->shutdown();
  delete client;
  
  // wait for messenger to finish
  rank.wait();
}



// reads a message type from the socket, and prints it.

int readmsgtype(int fd) {
  int rc;
  char typebuf[CMDLENGTH + 1];

  rc = read(fd, &typebuf, CMDLENGTH);
  
  // read a fixed-length text command
  if (rc != CMDLENGTH) {
    cerr << "in readmsgtype: read error: result is " << rc << endl;
    return -1;
  }

  // null-terminate the string
  typebuf[CMDLENGTH] = 0;

  // print the command
  //cerr << "readmsgtype: text type is " << typebuf << ", " ;

  // figure out which one it is, by number
  for (int i = 0; i < CMDCOUNT; ++i) {
    if (!strcmp(typebuf, CMD_LIST[i])) {
      //cerr << "which is identified as type " << i << endl;
      return i;
    } 
 }
  
  // if we get here the type was invalid
  cerr << "readmsgtype: unrecognized message type " << typebuf << endl;
  return -1;
}

// Attempt to read the message footer off
// the given stream.
bool check_footer(int fd) {

  // leave space for null termination
  char footer_buf[FOOTER_LENGTH+1];

  // read the footer
  int rc = read(fd, &footer_buf, FOOTER_LENGTH);
  if (rc != FOOTER_LENGTH) {
    cerr << "in check_footer: read error: result is " << rc << endl;
    return false;
  }

  // null-terminate the string
  footer_buf[FOOTER_LENGTH] = 0;

  // Is the footer correct?
  if (0 == strcmp(footer_buf, FOOTER))
    return true;
  else
    return false;
}


// send a fixed-length message header
// given the header's ID.
int send_msg_header(int fd, int header_ID) {
  if ((header_ID < 0) || (header_ID >= CMDCOUNT)) {
    cerr << "In send_msg_header: received out-of-range header ID " << header_ID <<
      ". Exiting process." << endl;
    exit(-1);
  }

  //cerr << "attempting to send message " << CMD_LIST[header_ID] << 
  //  " with ID " << header_ID << endl;

  if (CMDLENGTH != writen(fd, CMD_LIST[header_ID], CMDLENGTH)) {
    cerr << "In send_msg_header: error writing header ID " << header_ID << 
      "to file descriptor " << fd << ". Exiting process." << endl;
    exit(-1);
  }

  return 0;
}

// send the fixed-length message footer.
int send_msg_footer(int fd) {
  //cerr << "attempting to send message footer: " << endl;
  if (FOOTER_LENGTH != writen(fd, FOOTER, FOOTER_LENGTH)) {
    cerr << "in send_msg_footer: error writing footer to file descriptor " <<
      fd << ". Exiting process." << endl;
    exit(-1);
  } else {
    //cerr << "Sent message footer!" << endl; 
  }
  return 0;
}



// Copy a given extent of a Ceph file to the local disk.
// Requires a running Ceph client.
void copyExtentToLocalFile (Client* client, const char* ceph_source,
			    long offset, long length,
			    const char* local_destination) {

  // get the source file's size. Sanity-check the request range.
  struct stat st;
  int r = client->lstat(ceph_source, &st);
  assert (r == 0);

  off_t src_total_size = st.st_size;
  if (src_total_size < offset + length) {
    cerr << "Error in copy ExtentToLocalFile: offset + size = " << offset << " + " << length
	 << " = " + (offset + length) << ", source file size is only " << src_total_size << endl;
    exit(-1);
  }
  off_t remaining = length;

  // open the source and destination files. Advance the source
  // file to the desired offset.
  int fh_ceph = client->open(ceph_source, O_RDONLY);
  assert (fh_ceph > -1); 
  r = client->lseek(fh_ceph, offset, SEEK_SET);
  assert (r == offset);
  
  int fh_local = ::open(local_destination, O_WRONLY|O_CREAT|O_TRUNC, 0644);
  assert (fh_local > -1);

  // copy the file 4 MB at a time
  const int chunk = 4*1024*1024;
  bufferptr bp(chunk);

  while (remaining > 0) {
    off_t got = client->read(fh_ceph, bp.c_str(), MIN(remaining,chunk), -1);
    assert(got > 0);
    remaining -= got;
    off_t wrote = ::write(fh_local, bp.c_str(), got);
    assert (got == wrote);
  }
  // close the files
  client->close(fh_ceph);
  ::close(fh_local);
}


// Copy a Ceph file to the local disk. Requires a running Ceph client.
// Overwrites the destination if it exists.
void copyCephFileToLocalFile (Client* client, const char* ceph_source,
			      const char* local_destination) {

  // get the source file's size.
  struct stat st;
  int r = client->lstat(ceph_source, &st);
  assert (r == 0);
  
  copyExtentToLocalFile(client, ceph_source, 0, st.st_size,
			local_destination);
  

}
// Copies a file from the local disk to Ceph. Annihilates the
// destination if it exists.

void copyLocalFileToCeph(Client* client, const char* local_source,
			 const char* ceph_destination) {

  // Get the source file's size.
  struct stat st;
  int r = ::lstat(local_source, &st);
  if (0 != r) {
    cerr << "in copyLocalFileToCeph: error retrieving size for file " << local_source
	 << ": is the file missing?" << endl;
    assert(0);
  }

  off_t remaining = st.st_size;

  // Open the source and destination files.
  int fh_source = ::open(local_source, O_RDONLY);
  assert (fh_source > -1);
  int fh_dest = client->open(ceph_destination, O_WRONLY|O_CREAT|O_TRUNC, 0644);
  assert (fh_dest > -1);

  // Copy the file.
  const int chunk = 4 * 1024* 1024; // 4 MB
  bufferptr bp(chunk);

  while(remaining > 0) {
    off_t got = ::read(fh_source, bp.c_str(), MIN(remaining, chunk));
    assert(got > 0);
    remaining -= got;
    off_t wrote = client->write(fh_dest, bp.c_str(), got);
    assert (got == wrote);
  }

  // close the files
  ::close(fh_source);
  client->close(fh_dest);

}
