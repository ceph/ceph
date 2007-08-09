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


Client* startCephClient();
void kill_client(Client* client);


int readline(int fd, char *ptr, int maxlen);
int writen(int fd, const char *ptr, int nbytes);
int send_msg_header(int fd, int header_ID);
int readmsgtype(int fd);
bool check_footer(int fd);
int send_msg_header(int fd, int header_ID);
int send_msg_footer(int fd);
int read_positive_int(int fd);
long read_long(int fd);
string read_string(int fd, string *buf);
bool write_positive_int(int fd, int value);
off_t read_off_t(int fd);

/*
 * Fires up a Ceph client and returns a pointer to it.
 */ 

Client* startCephClient()
{
  dout(3) << "ActiveMaster: Initializing Ceph client:" << endl;
  
  // parse args from CEPH_ARGS, not command line 
  vector<char*> args; 
  env_to_vec(args);
  parse_config_options(args);

  if (g_conf.clock_tare) g_clock.tare();

  // be safe
  g_conf.use_abspaths = true;

  // load monmap
  MonMap monmap;
  int r = monmap.read(".ceph_monmap");
  if (r < 0) {
    dout(0) << "ActiveMaster: could not find .ceph_monmap" << endl; 
    return 0;
  }
  assert(r >= 0);

  // start up network
  rank.start_rank();

  // start client
  Client *client;
  client = new Client(rank.register_entity(MSG_ADDR_CLIENT_NEW), &monmap);
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


// Read n bytes from a descriptor.
int readn(int fd, char * ptr, int nbytes)
{
  int nleft, nread;

  nleft = nbytes;
  while (nleft > 0) {
    nread = read(fd, ptr, nleft);
    if (nread < 0)
      return nread; // error
    else if (nread == 0)
      break;

    nleft -= nread;
    ptr += nread;
  }
  return (nbytes - nleft);
}

// Read a line from the socket. This is horrifically slow, as
// it goes one character at a time to catch the newline.

int readline(int fd, char *ptr, int maxlen) {

  int n, rc;
  char c;

  for (n = 1; n < maxlen; ++n) {
    if ( (rc = read(fd, &c, 1)) == 1) {
      *ptr++ = c;
      if (c == '\n')
	break;
    } 
    else if (rc == 0) {
      if (n == 1)
	return 0; // EOF, no data
      else
	break; // EOF, data read
    }
    else
      return -1; // error

  }

  // null-terminate the string and return the number of bytes read
  *ptr = 0;
  return n;
}

// write n bytes to a stream file descriptor.

int writen(int fd, const char *ptr, int nbytes) {
  int nleft, nwritten;

  nleft = nbytes;

  // write until everything's sent
  while (nleft > 0) {
    nwritten = write(fd, ptr, nleft);
    if (nwritten <= 0) {
      cerr << "writen: error writing " << nbytes << 
	"bytes to file descriptor " << fd << endl;
      return nwritten; //error
    }
    nleft -= nwritten;
    ptr += nwritten;
  }
  assert (0 == nleft);
  return (nbytes - nleft);
}



// read a message type from the socket, and print it.

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


// Attempt to read a positive signed integer off the given stream.
// This function assumes that the sender and receiver use the same
// integer size. If this is false, weird stuff will happen.
int read_positive_int(int fd) {

  char buf[sizeof(int)];
  int rc = read(fd, &buf, sizeof(int));
  if (rc != sizeof(int)) {
    cerr << "in read_positive_int: read error: result is " << rc <<
	 ". Exiting process. " << endl;
    exit(-1);
  }
  return *((int*)buf);
}

long read_long(int fd) {
  char buf[sizeof(long)];
  int rc = read(fd, &buf, sizeof(long));
  if (rc != sizeof(long)) {
    cerr << "in read_long: read error: result is " << rc <<
	 ". Exiting process. " << endl;
    exit(-1);
  }
  return *((long*)buf);
}


off_t read_off_t(int fd) {
  char buf[sizeof(off_t)];
  int rc = read(fd, &buf, sizeof(off_t));
  if (rc != sizeof(off_t)) {
    cerr << "in read_off_t: read error: result is " << rc <<
	 ". Exiting process. " << endl;
    exit(-1);
  }
  return *((off_t*)buf);
}

size_t read_size_t(int fd) {
  char buf[sizeof(size_t)];
  int rc = read(fd, &buf, sizeof(size_t));
  if (rc != sizeof(size_t)) {
    cerr << "in read_size_t: read error: result is " << rc <<
	 ". Exiting process. " << endl;
    exit(-1);
  }
  return *((size_t*)buf);
}




// attempt to write an integer to the given
// file descriptor.
bool write_positive_int(int fd, int value) {
  
  char* buf = (char*)(&(value));
  int rc = writen(fd, buf, sizeof(int));
  
  if (rc != sizeof(int)) {
    cerr << "in write_positive_int: write failed, result is " << rc <<
      ", sizeof(int) is " << sizeof(int) << ". Exiting process." << endl;
    exit(-1);
  }

  return true;
}

// attempt to write a long integer to the given
// file descriptor.
bool write_long(int fd, long value) {
  
  char* buf = (char*)(&(value));
  int rc = writen(fd, buf, sizeof(long));
  
  if (rc != sizeof(long)) {
    cerr << "in write_long: write failed, result is " << rc <<
      ", sizeof(long) is " << sizeof(long) << ". Exiting process." << endl;
    exit(-1);
  }

  return true;
}


// attempt to write a long integer to the given
// file descriptor.
bool write_off_t(int fd, off_t value) {
  
  char* buf = (char*)(&(value));
  int rc = writen(fd, buf, sizeof(off_t));
  
  if (rc != sizeof(off_t)) {
    cerr << "in writeoff_t: write failed, result is " << rc <<
      ", sizeof(off_t) is " << sizeof(off_t) << ". Exiting process." << endl;
    exit(-1);
  }

  return true;
}




// read a string from the given file descriptor.
// The expected format is an int n denoting the
// length of the string, followed by a series of n
// bytes, not null-terminated.
void read_string(int fd, char* buf) {  

  // get the size of the string
  int size = read_positive_int(fd);
  if (size < 1) {
    cerr << "Error in read_string: invalid string size of " << size << endl;
    exit(-1);
      }
  if (size > MAX_STRING_SIZE) {
    cerr << "Error in read_string: string size of " << size << "is more than maximum of"
	 << MAX_STRING_SIZE << endl;
    exit(-1);
  }

  // read the string
  int result = readn(fd, buf, size);
  if (result != size) {
    cerr << "Error in read_string: attempted read size was " << size << 
      ", result was " << result << endl;
    exit(-1);
  }
  // null-terminate
  buf[size] = 0;

  cerr << "in read_string: read string \"" << buf << "\" of size " << size << endl;

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


// Writes a string to a stream file descriptor.
// Dies loudly and horribly on any error.
bool write_string(int fd, const char* buf) {

  int length = strlen(buf);
  assert (length >= 0);
  int result = writen(fd, buf, length);
  if (result != length) {
    cerr << "Error in write_string: string length is " << length << 
      ", result is " << result << endl;
    exit(-1);
  }

  return true;
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
