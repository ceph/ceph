
#include "include/MDLog.h"
#include "include/LogStream.h"
#include "include/LogEvent.h"

// cons/des

MDLog::MDLog() 
{
  num_events = 0;
  reader = new LogStream(666,666);
  writer = new LogStream(666,666);
}



int MDLog::submit_entry( LogEvent *e,
						 Context *c ) 
{
  // write it
  writer->append(e, c);
}


class C_MDL_Trim : public Context {
protected:
  MDLog *mdl;
  Context *con;
public:
  LogEvent *le;

  C_MDL_Trim(MDLog *m, Context *c) {
	mdl = m; con = c; 
  }
  void finish(int res) {
	mdl->trim_2(le, 
				con);
  }
};

int MDLog::trim(Context *c)
{
  while (trimming.size() + num_events > max_events) {
	// pull off an event
	C_MDL_Trim *n = new C_MDL_Trim(this, c);  
	reader->read_next(&n->le, n);
  }
}

int MDLog::trim_2(LogEvent *e,
				  Context *c)
{
  /*
  if (e.discardable()) {
	// discard

  } else {
  e.retire(new C_MDL_TrimFinisher(c));	
  }
  */
  // add to limbo list
  trimming.push_back(e);

  // 

}
