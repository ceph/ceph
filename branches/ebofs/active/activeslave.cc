/*
 * This is a slave for receiving and executing commands for 
 * compute tasks on an OSD. This supersedes the daemon
 * version in activetaskd.h/cc, because it's easier to debug
 * if it's not a daemon.
 *
 * Networking code is based off examples from Stevens' UNIX Network Programming.
 */

#include "activeslave.h"

int main(int argc, const char* argv[]) {

  /* Set up TCP server */
  int sockfd, newsockfd,  childpid;
  socklen_t clilen;
  struct sockaddr_in cli_addr, serv_addr;

  //const char *pname = argv[0]; // process name

  // Open a TCP socket
  if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    cerr << "slave: can't open TCP socket. Exiting." << endl;
    exit(-1);
  }
  cerr << "slave: opened TCP socket." << endl;

  // set up the port
  bzero((char*) &serv_addr, sizeof(serv_addr));
  serv_addr.sin_family      = AF_INET;
  serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
  serv_addr.sin_port        = htons(SERV_TCP_PORT);

  if (bind(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
    cerr << "slave: can't bind local address. Exiting." << endl;
    exit(-1);
  } 

  if(listen(sockfd, SOMAXCONN) < 0) {
    cerr << "slave: listening error. Exiting." << endl;
    exit(-1);
  }


  /* The Big Loop */
  while (1) {

    // wait for a message and fork off a child process to handle it
    clilen = sizeof(cli_addr);
    newsockfd = accept(sockfd,
		       (struct sockaddr *) &cli_addr,
		       &clilen);

    if (newsockfd < 0) {
      cerr << "slave: accept error. Exiting." << endl;
      exit(-1);
    }

    if ((childpid = fork()) < 0) {
      cerr << "slave: fork error. Exiting." << endl;
      exit(-1);
    }

    else if (childpid == 0) { // child process
      cerr << "Forked child process for incoming socket" << endl;
      close(sockfd);
      process_request(newsockfd);
      cerr << "Finished processing request. Exiting child." << endl;
      exit(0);
    }
    
    close (newsockfd); // parent

    //sleep(30); /* wait 30 seconds */
  }
  exit(EXIT_SUCCESS);
}


/* This will process requests from the master.
 * The protocol in a nutshell:
 *   Master opens a socket to slave, and sends
 * one message.
 *   Slave replies with one message.
 *   Socket is closed.
 */

void process_request(int newsockfd) {

  // first, read the message type.
  int msg_type = readmsgtype(newsockfd);
  
  
  // Second, call some function based on the message type to process
  // the rest of the message. The function is responsible for the rest
  // of the message; this includes checking the message footer.

  switch(msg_type) {

  case PING: // ping
    process_ping(newsockfd);
    break;
  case STARTTASK: // start_task
    process_start_task(newsockfd);
    break;
  case RETRIEVELOCALFILE: // get_local
    process_get_local(newsockfd);
    break;
  case SHIPCODE:
    process_shipcode(newsockfd);
    break;

  case PINGREPLY:
  case FINISHEDTASK:
  case TASKFAILED:
  case SENDLOCALFILE:
  case LOCALFILENOTFOUND:
  case CODESAVED:
  case SHIPFAILED:
    cerr << "activeslave: BUG received message " << CMD_LIST[msg_type] <<
      " from master; master should never send this message." << endl;
    exit(-1);
    break;
    

  case -1:
    cerr << "activeslave: message had an unidentifiable type. " <<
      "Closing socket and discarding rest of message." << endl;
  default:
    cerr << "activeslave: BUG! received unexpected return value of" << msg_type <<
      "from readmsgtype(). Closing socket and discarding rest of message." << endl;

    exit(-1);
  }
}


// Just write a ping_reply to the socket.
void process_ping(int fd) {

  // make sure the footer is valid
  if (!check_footer(fd)) {
    cerr << "process_ping warning: ping message has invalid or missing footer."
	 << endl;
  }
  // Even if the footer's invalid, send the reply. 
  cerr << "Replying to ping..." << endl;
  send_msg_header(fd, PINGREPLY);
  send_msg_footer(fd);
  cerr << "Ping processing completed." << endl;
}



// Process a start_task message. This reads the incoming message and
// starts the corresponding task.

// Parameter format: taskID(int) command(string) 
// cephinputfile(string) offset(long) length(long) localoutputfile

// WARNING: currently has the trivial task hardwired. It
// ignores the command and the output file.
void process_start_task(int fd) {

  char command[MAX_STRING_SIZE + 1];
  char cephinputfile[MAX_STRING_SIZE + 1];
  char localoutputfile[MAX_STRING_SIZE + 1];

  cout << "in process_start_task: ";
  int taskID = read_positive_int(fd);
  cout << "read taskID " << taskID;

  read_string(fd, command);
  cout << ", command " << command;

  read_string(fd, cephinputfile);
  cout << ", cephinputfile " << cephinputfile;
  off_t offset = read_off_t(fd);
  cout << ", offset " << offset;
  off_t length = read_off_t(fd);
  cout << ", length " << length;

  read_string(fd, localoutputfile);
  cout << ", localoutputfile " << localoutputfile << endl;

  // make sure the footer is valid
  if (!check_footer(fd)) {
    cerr << "process_start_task warning: message has invalid or missing footer. "
	 << "Discarding message." << endl;
    exit(-1);
  }
  

  // To do: modify to load the task from a library instead of just
  // using the hardwired one.

  void (*task)(const char*, const char*, off_t, off_t) = 0;
  task = start_trivial_task;


 
  // start a task; create an output filename that uses the task ID, 'cause we might
  // end up with multiple pieces of a file on each OSD.
  // WARNING: always does the trivial task; prints answer to stdout but
  // does not write it to disk.
  cerr << "starting task: " << endl;
  //start_trivial_task(cephinputfile, localoutputfile, offset, length);
  task(cephinputfile, localoutputfile, offset, length);
  cerr << "returned from task! Sending reply:" << endl;



  // send the reply
  send_msg_header(fd, FINISHEDTASK);
  write_positive_int(fd, taskID);
  send_msg_footer(fd);

  // done
  cout << "Done sending reply for taskID " << taskID << endl;
  return;
}



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


// Starts a sloppy grep count of the hardwired search string over the
// given Ceph file extent. It's sloppy because it copies the given
// extent to a local file and runs "grep" on it, with no effort to take
// care of boundary issues.
void start_sloppy_grepcount (const char* ceph_filename, const char* local_filename,
			     long offset, long size) {

  Client* client = startCephClient();
  char* search_string = "the";
  // copy the file to a local file. 

  copyExtentToLocalFile (client, ceph_filename, offset, size, local_filename);
  // we want: grep -c search_string local_filename
  // to get the number of occurrences of the string.
  string command = "";
  command.append("grep -c ");
  command.append(search_string);
  command.append(local_filename);

  assert(0);

}


// Processes a SHIPCODE message. The message will have a shared
// library attached to it, which must be stored locally.

void process_shipcode(int fd) {


  // get the size of the shared library
  size_t library_size = read_size_t(fd);


  // save the library to a file
  cerr << "saving library..." << endl;
  const char* libfile = "/tmp/libslavetask.so";
  int local_fd = ::open(libfile, O_WRONLY | O_CREAT | O_TRUNC);
  if (local_fd < 0) {
    cerr << "Error opening " << libfile << " for writing." << endl;
    exit(-1);
  }

  off_t remaining = library_size;

  bufferptr bp(CHUNK);
  char* bp_c = bp.c_str();
  while (remaining > 0) {
    off_t got = readn(fd, bp_c, MIN(remaining, CHUNK));
    assert(got > 0);
    remaining -= got;
    ssize_t written = ::write(local_fd, bp_c, got);
    assert (written == got);
  }
  cerr << "Received shared library and stored as " << libfile << endl;

}


// Processes a get_local message. The message
// specifies the filename of a local file to
// return to the sender.

// Parameter format: requestID(int) localfilename(string)

// INCOMPLETE: currently just reads the message.


void process_get_local(int fd) {
  cout << "in process_get_local: ";
  int taskID = read_positive_int(fd);
  cout << "read taskID " << taskID;

  char localfilename[MAX_STRING_SIZE+1];
  read_string(fd, localfilename);
  cout << ", localfilename " << localfilename << endl;


  // make sure the footer is valid
  if (!check_footer(fd)) {
    cerr << "process_get_local warning: message has invalid or missing footer."
	 << endl;
  }

  // not implemented
  cerr << "Error: get_local command unimplemented." << endl;
  assert(0);
}


// Retrieves a formatted message from the socket.
// At the moment, this just reads and prints a fixed-
// length message type.
// DEPRECATED.
void str_getmsg(int sockfd) {
  
  int  n;

  // read message types until the connection dies
  while(true) {
    n = readmsgtype(sockfd);
    if (n != 0) {
      cerr << "from getmsg: some sort of error" << endl;
      exit(-1);
    }
  }
}

// Echo a stream socket message back to the sender.
// DEPRECATED.
void str_echo(int sockfd) {
  
  int  n;
  char line[MAXLINE];

  while(true) {

    // read from the stream
    cerr << "str_echo: waiting for a line" << endl;
    n = readline(sockfd, line, MAXLINE);
    cerr << "str_echo: read a line" << endl;
    if (0 == n) {
      cerr << "str_echo: connection terminated" << endl;
      return; // connection is terminated
    }
    else if (n < 0) {
      cerr << "str_echo: readline error" << endl;
      exit(-1);
    }

    // write back to the stream
    if (n != writen(sockfd, line, n)) {
      cerr << "str_echo: writen error" << endl;
      exit(-1);
    }
    else
      cerr << "Echoed line " << endl;
  }
}


void str_ack(int sockfd) {
  
  int  n;
  char line[MAXLINE];
  //char *ack = "ack";

  while(true) {

    // read from the stream
    n = readline(sockfd, line, MAXLINE);

    if (0 == n)
      return; // connection is terminated
    else if (n < 0)
      //err_dump("str_echo: readline error");
      exit(-1);

    // write back to the stream
    if (4 != writen(sockfd, "ack\n", 4))
      //err_dump("str_echo: writen error");
      exit(-1);
  }
}



// Read command lines from the socket and execute them

void str_run(int sockfd) {
  
  int  n;
  char line[MAXLINE];
  char* error_msg = "str_run: No command interpreter found\n";
  char* ack_msg = "Running command... ";
  char* commit_msg = "Command executed!\n";

  while(true) {

    // read from the stream
    n = readline(sockfd, line, MAXLINE);

    if (0 == n)
      return; // connection is terminated
    else if (n < 0)
      //err_dump("str_echo: readline error");
      exit(-1);

    if (system(NULL)) {
      writen(sockfd, ack_msg, strlen(ack_msg));
      system(line);
      writen(sockfd, commit_msg, strlen(commit_msg));
    }
    else if ((int)strlen(error_msg) != writen(sockfd, error_msg, strlen(error_msg))) 
      //err_dump("str_echo: writen error");
      exit(-1);
  }
}


// take a filename and copy it from Ceph to a local directory.
// Not completed.

void str_copytolocal(int sockfd) {
  
  int  n;
  char line[MAXLINE];
  char* error_msg = "str_copy: No command interpreter found\n";
  char* ack_msg = "Running command... ";
  char* commit_msg = "Command executed!\n";
  //char* temp_dir = "/tmp";


  while(true) {

    // read from the stream
    n = readline(sockfd, line, MAXLINE);

    if (0 == n)
      return; // connection is terminated
    else if (n < 0)
      //err_dump("str_echo: readline error");
      exit(-1);

    if (system(NULL)) {
      writen(sockfd, ack_msg, strlen(ack_msg));
      system(line);
      writen(sockfd, commit_msg, strlen(commit_msg));
    }
    else if ((int)strlen(error_msg) != writen(sockfd, error_msg, strlen(error_msg))) 
      //err_dump("str_echo: writen error");
      exit(-1);
  }
}



