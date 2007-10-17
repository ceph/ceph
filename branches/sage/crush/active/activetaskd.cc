/*
 * This is a daemon for receiving and executing commands for compute tasks on an OSD.
 *
 * The daemon uses skeleton code from
 * http://www.linuxprofilm.com/articles/linux-daemon-howto.html. The
 * site is no longer up, but can be seen through the archive.org.
 * Networking code is based off examples from Stevens' UNIX Network Programming.
 */

#include "activetaskd.h"


#define SERVER

#undef SERVER

int main(int argc, const char* argv[]) {
        
  /* Our process ID and Session ID */
  pid_t pid, sid;
        
  /* Fork off the parent process */
  pid = fork();
  if (pid < 0) {
    exit(EXIT_FAILURE);
  }
  /* If we got a good PID, then
     we can exit the parent process. */
  if (pid > 0) {
    exit(EXIT_SUCCESS);
  }

  /* Change the file mode mask */
  umask(0);
                
  /* Open any logs here */        
                
  /* Create a new SID for the child process */
  sid = setsid();
  if (sid < 0) {
    /* Log the failure */
    exit(EXIT_FAILURE);
  }
        
        
  /* Change the current working directory */
  if ((chdir("/")) < 0) {
    /* Log the failure */
    exit(EXIT_FAILURE);
  }
        
  /* Close out the standard file descriptors */
  close(STDIN_FILENO);
  close(STDOUT_FILENO);
  close(STDERR_FILENO);
        
  /* Daemon-specific initialization goes here */



  /* Set up TCP server */
  int sockfd, newsockfd,  childpid;
  socklen_t clilen;
  struct sockaddr_in cli_addr, serv_addr;

  const char *pname = argv[0]; // process name

  // Open a TCP socket
  if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    exit(-1);
  //err_dump("server: can't open stream socket");

  // set up the port
  bzero((char*) &serv_addr, sizeof(serv_addr));
  serv_addr.sin_family      = AF_INET;
  serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
  serv_addr.sin_port        = htons(SERV_TCP_PORT);

  if (bind(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0)
    exit(-1);
    //err_dump("server: can't bind local address");

  if(listen(sockfd, SOMAXCONN) < 0)
    exit(-1);
  //err_dump("server: listening error");

  /* The Big Loop */
  while (1) {

    // wait for a message and fork off a child process to handle it
    clilen = sizeof(cli_addr);
    newsockfd = accept(sockfd,
		       (struct sockaddr *) &cli_addr,
		       &clilen);

    if (newsockfd < 0)
      exit(-1);
      //err_dump("server: accept error");

    if ( (childpid = fork()) < 0)
      exit(-1);
    // err_dump("server: fork error");

    else if (childpid == 0) { // child process
      close(sockfd);
      //str_echo(newsockfd);
      str_run(newsockfd);
      // insert code to process the request
      exit(0);
    }
    
    close (newsockfd); // parent

    //sleep(30); /* wait 30 seconds */
  }
  exit(EXIT_SUCCESS);
}


// Echo a stream socket message back to the sender.

void str_echo(int sockfd) {
  
  int  n;
  char line[MAXLINE];

  while(true) {

    // read from the stream
    n = readline(sockfd, line, MAXLINE);

    if (0 == n)
      return; // connection is terminated
    else if (n < 0)
      //err_dump("str_echo: readline error");
      exit(-1);

    // write back to the stream
    if (n != writen(sockfd, line, n)) 
      //err_dump("str_echo: writen error");
      exit(-1);
  }
}


void str_ack(int sockfd) {
  
  int  n;
  char line[MAXLINE];
  char *ack = "ack";

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
    else if (strlen(error_msg) != writen(sockfd, error_msg, strlen(error_msg))) 
      //err_dump("str_echo: writen error");
      exit(-1);
  }
}


// take a filename and copy it from Ceph to a local directory

void str_copytolocal(int sockfd) {
  
  int  n;
  char line[MAXLINE];
  char* error_msg = "str_copy: No command interpreter found\n";
  char* ack_msg = "Running command... ";
  char* commit_msg = "Command executed!\n";
  char* temp_dir = "/tmp";


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
    else if (strlen(error_msg) != writen(sockfd, error_msg, strlen(error_msg))) 
      //err_dump("str_echo: writen error");
      exit(-1);
  }
}



