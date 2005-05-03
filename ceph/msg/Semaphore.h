/*

don't use this.. you probably want a pthread Cond instead!

*/

/////////////////////////////////////////////////////////////////////
//  Written by Phillip Sitbon
//  Copyright 2003
//
//  Posix/Semaphore.h
//    - Resource counting mechanism
//
/////////////////////////////////////////////////////////////////////

#ifndef _Semaphore_Posix_
#define _Semaphore_Posix_

#include <semaphore.h>
#include <errno.h>
#include <iostream>
using namespace std;

class Semaphore
{
  sem_t S;

  public:
  Semaphore( int init = 0 ) { 
	int r = sem_init(&S,0,init); 
	//cout << "sem_init = " << r << endl;
  }

  virtual ~Semaphore() { 
	int r = sem_destroy(&S); 
	//cout << "sem_destroy = " << r << endl;
  }

  void Wait() const {
	while (1) {
	  int r = sem_wait((sem_t *)&S); 
	  if (r == 0) break;
	  cout << "sem_wait returned " << r << ", trying again" << endl;
	}
  }

  int Wait_Try() const
  { return (sem_trywait((sem_t *)&S)?errno:0); }

  int Post() const
  { return (sem_post((sem_t *)&S)?errno:0); }

  int Value() const
  { int V = -1; sem_getvalue((sem_t *)&S,&V); return V; }

  void Reset( int init = 0 )
  { sem_destroy(&S); sem_init(&S,0,init); }
};

#endif // !_Semaphore_Posix_
