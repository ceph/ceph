
#ifndef __MDLOG_H
#define __MDLOG_H

/*

Things that go in the MDS log:


prepare + commit versions of many of these?

- inode update
   entry will include mdloc_t of dir it resides in...

- directory operation
  unlink,
  rename= atomic link+unlink (across multiple dirs, possibly...)

- import
- export


*/


class MDLog {
 protected:

  
  
 public:
  
  void submit_entry( MDLogEntry *e,
					 Context *c );
  
  void trim();


};
