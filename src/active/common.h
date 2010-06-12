#ifndef CEPH_COMMON_H
#define CEPH_COMMON_H


#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <syslog.h>
#include <string.h>
#include <iostream>
#include <string>




#define CMDLENGTH 10
#define CMDCOUNT 11

#define MAX_STRING_SIZE 255

/*
 * These are the various messages that can be sent between the master
 * and slave. The slave sends one reply to each message from the master.

 * PING/PINGREPLY: just what it sounds like.

 * STARTTASK: starts a task from the given library. The slave attempts
 * to perform the task, and replies with FINISHEDTASK or TASKFAILED.
 *
 * RETRIEVELOCALFILE: requests a file that the slave has stored
 * locally. Slave replies with SENDLOCALFILE and the file, or with
 * LOCALFILENOTFOUND.
 * 
 * (deprecated) SHIPCODE: sends a shared library to the slave, containing a
 * function that is to be executed later by the STARTTASK
 * command. Slave replies with CODESAVED or SHIPFAILED.
 *
 * SHIPCODE is replaced by transport using Ceph.
 */ 


const off_t CHUNK = 1024 * 1024 * 4;

// a bunch of string constants for commands

#define PING              0
#define STARTTASK         1
#define RETRIEVELOCALFILE 2
#define PINGREPLY         3
#define FINISHEDTASK      4
#define TASKFAILED        5
#define SENDLOCALFILE     6
#define LOCALFILENOTFOUND 7
#define SHIPCODE          8
#define CODESAVED         9
#define SHIPFAILED        10

#define FOOTER_LENGTH 7

const char* CMD_LIST[CMDCOUNT] = {"______PING",
				     "START_TASK",
				     "_GET_LOCAL",
				     "PING_REPLY",
				     "_TASK_DONE",
				     "TASKFAILED",
				     "SEND_LOCAL",
				     "LOCAL_GONE",
				     "_SHIP_CODE",
				     "CODE_SAVED",
				     "SHIPFAILED"};

const char FOOTER[FOOTER_LENGTH + 1] = "MSG_END";

// const char* strArray[] = {"string1", "string2", "string3"};
//const char commands[2][4]  = {"foo", "bar"};

 
// error codes
#define ARGUMENTSINVALID 1001
#define CEPHCLIENTSTARTUPFAILED 1002
#define INPUTFILEREADFAILED 1003

#endif //COMMON_H
