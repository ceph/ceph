
#include "NewMessenger.h"

#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "config.h"

#include "messages/MGenericMessage.h"
#include "messages/MNSConnect.h"
#include "messages/MNSConnectAck.h"
#include "messages/MNSRegister.h"
#include "messages/MNSRegisterAck.h"
#include "messages/MNSLookup.h"
#include "messages/MNSLookupReply.h"

#include <netdb.h>


#define DBL 0

#undef dout
#define dout(l)  if (l<=g_conf.debug) cout << "rank" << rank.my_rank << "."



#include "tcp.cc"


/********************************************
 * Namer
 */

Rank::Namer::Namer(EntityMessenger *msgr) :
  messenger(msgr),
  nrank(0), nclient(0), nmds(0), nosd(0), nmon(0)
{
  // me
  /*msg_addr_t whoami = msgr->get_myaddr();
  rank.entity_rank[whoami] = rank.my_rank;
  rank.rank_addr[0] = rank.accepter.listen_addr;
  */

  assert(rank.my_rank == 0);
  nrank = 1;
  
  // announce myself
  cout << "export CEPH_NAMESERVER=" << rank.accepter.listen_addr << endl;
  int fd = ::open(".ceph_ns", O_WRONLY|O_CREAT);
  ::write(fd, (void*)&rank.accepter.listen_addr, sizeof(tcpaddr_t));
  ::fchmod(fd, 0755);
  ::close(fd);

  // ok
  messenger->set_dispatcher(this);
  messenger->ready();
}

Rank::Namer::~Namer()
{
  ::unlink(".ceph_ns");
}


void Rank::Namer::handle_connect(MNSConnect *m)
{
  int newrank = nrank++;
  dout(2) << "namer.handle_connect from new rank " << newrank << " " << m->get_addr() << endl;
  
  rank.entity_rank[MSG_ADDR_RANK(newrank)] = newrank;
  rank.rank_addr[newrank] = m->get_addr();

  messenger->send_message(new MNSConnectAck(newrank),
							   MSG_ADDR_RANK(newrank));
  delete m;
}

void Rank::Namer::handle_register(MNSRegister *m)
{
  dout(10) << "namer.handle_register from rank " << m->get_rank()
		   << " addr " << MSG_ADDR_NICE(m->get_entity()) << endl;
  
  // pick id
  int newrank = m->get_rank();
  msg_addr_t entity = m->get_entity();

  if (entity.is_new()) {
	// make up a new address!
	switch (entity.type()) {
	  
	case MSG_ADDR_RANK_BASE:         // stupid client should be able to figure this out
	  entity = MSG_ADDR_RANK(newrank);
	  break;
	  
	case MSG_ADDR_MDS_BASE:
	  entity = MSG_ADDR_MDS(nmds++);
	  break;
	  
	case MSG_ADDR_OSD_BASE:
	  entity = MSG_ADDR_OSD(nosd++);
	  break;
	  
	case MSG_ADDR_CLIENT_BASE:
	  entity = MSG_ADDR_CLIENT(nclient++);
	  break;
	  
	default:
	  assert(0);
	}
  } else {
	// specific address!
  }

  dout(2) << "namer.handle_register registering " << entity << endl;

  // register
  assert(rank.entity_rank.count(entity) == 0);  // make sure it doesn't exist yet.
  rank.entity_rank[entity] = newrank;
  
  //++version;
  //update_log[version] = entity;
  
  // reply w/ new id
  messenger->send_message(new MNSRegisterAck(m->get_tid(), entity), 
						  MSG_ADDR_RANK(newrank));

  // anybody waiting?
  if (waiting.count(entity)) {
	list<Message*> ls;
	ls.swap(waiting[entity]);
	waiting.erase(entity);

	dout(10) << "doing waiters on " << entity << endl;
	for (list<Message*>::iterator it = ls.begin();
		 it != ls.end();
		 it++) 
	  dispatch(*it);
  }

  delete m;
}

void Rank::Namer::handle_unregister(Message *m)
{
  msg_addr_t who = m->get_source();
  dout(2) << "namer.handle_unregister from entity " << who << endl;
  
  assert(rank.entity_rank.count(who));
  rank.entity_rank.erase(who);

  /*
  // shutdown?
  if (dir.size() <= 2) {
	dout(2) << "dir is empty except for me, shutting down" << endl;
	tcpmessenger_stop_nameserver();
  }
  else {
	if (0) {
	  dout(10) << "dir size now " << dir.size() << endl;
	  for (hash_map<msg_addr_t, int>::iterator it = dir.begin();
		   it != dir.end();
		   it++) {
		dout(10) << " dir: " << MSG_ADDR_NICE(it->first) << " on rank " << it->second << endl;
	  }
	}
  }
  */
}


void Rank::Namer::handle_lookup(MNSLookup *m) 
{
  // have it?
  if (rank.entity_rank.count(m->get_entity()) == 0) {
	dout(DBL) << "namer " << m->get_source() << " lookup '" << m->get_entity() << "' -> dne" << endl;
	waiting[m->get_entity()].push_back(m);
	return;
  }

  // look it up!  
  MNSLookupReply *reply = new MNSLookupReply(m);

  int trank = rank.entity_rank[m->get_entity()];
  reply->entity_map[m->get_entity()] = trank;
  reply->rank_addr[trank] = rank.rank_addr[trank];

  dout(DBL) << "namer " << m->get_source() << " lookup '" << m->get_entity() << "' -> rank " << trank << endl;

  messenger->send_message(reply,
						  m->get_source(), m->get_source_port());
  delete m;
}



/********************************************
 * Accepter
 */

int Rank::Accepter::start()
{
  // bind to a socket
  dout(DBL) << "accepter.start binding to listen " << endl;
  
  /* socket creation */
  listen_sd = socket(AF_INET,SOCK_STREAM,0);
  assert(listen_sd > 0);
  
  /* bind to port */
  memset((char*)&listen_addr, 0, sizeof(listen_addr));
  listen_addr.sin_family = AF_INET;
  listen_addr.sin_addr.s_addr = htonl(INADDR_ANY);
  listen_addr.sin_port = 0;
  
  int rc = bind(listen_sd, (struct sockaddr *) &listen_addr, sizeof(listen_addr));
  assert(rc >= 0);

  socklen_t llen = sizeof(listen_addr);
  getsockname(listen_sd, (sockaddr*)&listen_addr, &llen);
  
  int myport = listen_addr.sin_port;

  // listen!
  rc = ::listen(listen_sd, 1000);
  assert(rc >= 0);

  dout(DBL) << "listening on " << myport << endl;
  
  // my address is...
  char host[100];
  gethostname(host, 100);
  dout(DBL) << "my hostname is " << host << endl;

  struct hostent *myhostname = gethostbyname( host ); 

  struct sockaddr_in my_addr;  
  memset(&my_addr, 0, sizeof(my_addr));

  my_addr.sin_family = myhostname->h_addrtype;
  memcpy((char *) &my_addr.sin_addr.s_addr, 
		 myhostname->h_addr_list[0], 
		 myhostname->h_length);
  my_addr.sin_port = myport;
  
  listen_addr = my_addr;
  
  dout(DBL) << "listen addr is " << listen_addr << endl;

  // start thread
  create();

  return 0;
}

void *Rank::Accepter::entry()
{
  dout(DBL) << "accepter starting" << endl;

  while (!done) {
	// accept
	struct sockaddr_in addr;
	socklen_t slen = sizeof(addr);
	int sd = ::accept(listen_sd, (sockaddr*)&addr, &slen);
	if (sd > 0) {
	  dout(DBL) << "accepted incoming on sd " << sd << endl;
	  
	  Receiver *r = new Receiver(sd);
	  r->create();
	  
	  rank.lock.Lock();
	  rank.receivers.insert(r);
	  rank.lock.Unlock();
	} else {
	  dout(DBL) << "no incoming connection?" << endl;
	  break;
	}
  }

  return 0;
}


/**************************************
 * Receiver
 */

void *Rank::Receiver::entry()
{
  while (!done) {
	Message *m = read_message();
	if (!m) {
	  ::close(sd);
	  break;
	}
	
	// find entity
	EntityMessenger *entity = 0;
	rank.lock.Lock();
	{
	  if (rank.local.count(m->get_dest()))
		entity = rank.local[m->get_dest()];
	}
	rank.lock.Unlock();
	
	assert(entity);

	// queue
	entity->queue_message(m);
  }
  
  return 0;
}

Message *Rank::Receiver::read_message()
{
  // envelope
  dout(DBL) << "receiver.read_message from sd " << sd  << endl;
  
  msg_envelope_t env;
  if (!tcp_read( sd, (char*)&env, sizeof(env) ))
	return 0;
  
  if (env.type == 0) {
	dout(DBL) << "receiver got dummy env, bailing" << endl;
	return 0;
  }

  dout(DBL) << "receiver got envelope type=" << env.type 
			<< " src " << env.source << " dst " << env.dest
			<< " nchunks=" << env.nchunks
			<< endl;
  
  // payload
  bufferlist blist;
  for (int i=0; i<env.nchunks; i++) {
	int size;
	tcp_read( sd, (char*)&size, sizeof(size) );
	
	bufferptr bp = new buffer(size);
	
	if (!tcp_read( sd, bp.c_str(), size )) return 0;
	
	blist.push_back(bp);
	
	dout(DBL) << "receiver got frag " << i << " of " << env.nchunks 
			  << " len " << bp.length() << endl;
  }
  
  // unmarshall message
  size_t s = blist.length();
  Message *m = decode_message(env, blist);
  
  dout(DBL) << "reciever got " << s << " byte message from " 
			<< m->get_source() << endl;
  
  return m;
}


/**************************************
 * Sender
 */

int Rank::Sender::connect()
{
  dout(DBL) << "sender.connect to " << tcpaddr << endl;

  // create socket?
  sd = socket(AF_INET,SOCK_STREAM,0);
  assert(sd > 0);
  
  // bind any port
  struct sockaddr_in myAddr;
  myAddr.sin_family = AF_INET;
  myAddr.sin_addr.s_addr = htonl(INADDR_ANY);
  myAddr.sin_port = htons( 0 );    
  
  int rc = bind(sd, (struct sockaddr *) &myAddr, sizeof(myAddr));
  assert(rc>=0);

  // connect!
  int r = ::connect(sd, (sockaddr*)&tcpaddr, sizeof(myAddr));
  assert(r >= 0);

  // identify myself
  // FIXME
  
  return 0;
}

void *Rank::Sender::entry()
{
  lock.Lock();
  while (!q.empty() || !done) {
	
	if (!q.empty()) {
	  dout(DBL) << "sender grabbing message(s)" << endl;
	  
	  // grab outgoing list
	  list<Message*> out;
	  out.swap(q);
	  
	  // drop lock while i send these
	  lock.Unlock();
	  
	  while (!out.empty()) {
		Message *m = out.front();
		out.pop_front();
		
		// marshall
		if (m->empty_payload())
		  m->encode_payload();

		write_message(m);
	  }
	  
	  lock.Lock();
	  continue;
	}
	
	// wait
	dout(DBL) << "sender sleeping" << endl;
	cond.Wait(lock);
  }
  dout(DBL) << "sender done" << endl;
  
  lock.Unlock();  
  return 0;
}


void Rank::Sender::write_message(Message *m)
{
  // get envelope, buffers
  msg_envelope_t *env = &m->get_envelope();
  bufferlist blist;
  blist.claim( m->get_payload() );
  
#ifdef TCP_KEEP_CHUNKS
  env->nchunks = blist.buffers().size();
#else
  env->nchunks = 1;
#endif

  dout(DBL) << g_clock.now() << " sending " << m << " " << *m 
			<< " to " << m->get_dest()
			<< endl;
  
  // send envelope
  int r = tcp_write( sd, (char*)env, sizeof(*env) );
  if (r < 0) { 
	cerr << "error sending envelope for " << *m
		 << " to " << m->get_dest() << endl; assert(0); 
  }

  // payload
#ifdef TCP_KEEP_CHUNKS
  // send chunk-wise
  int i = 0;
  for (list<bufferptr>::iterator it = blist.buffers().begin();
	   it != blist.buffers().end();
	   it++) {
	dout(DBL) << "tcp_sending frag " << i << " len " << (*it).length() << endl;
	int size = (*it).length();
	r = tcp_write( sd, (char*)&size, sizeof(size) );
	if (r < 0) { cerr << "error sending chunk len for " << *m << " to " << m->get_dest() << endl; assert(0); }
	r = tcp_write( sd, (*it).c_str(), size );
	if (r < 0) { cerr << "error sending data chunk for " << *m << " to " << m->get_dest() << endl; assert(0); }
	i++;
  }
#else
  // one big chunk
  int size = blist.length();
  r = tcp_write( sd, (char*)&size, sizeof(size) );
  if (r < 0) { cerr << "error sending data len for " << *m << " to " << m->get_dest() << endl; assert(0); }
  for (list<bufferptr>::iterator it = blist.buffers().begin();
	   it != blist.buffers().end();
	   it++) {
	r = tcp_write( sd, (*it).c_str(), (*it).length() );
	if (r < 0) { cerr << "error sending data megachunk for " << *m << " to " << m->get_dest() << " : len " << (*it).length() << endl; assert(0); }
  }
#endif
  
  // delete message
  delete m;
}



/********************************************
 * Rank
 */

Rank::Rank(int r) : 
  my_rank(r),
  namer(0) {
}
Rank::~Rank()
{
  //FIXME
  if (namer) delete namer;
}


int Rank::start_rank(tcpaddr_t& ns)
{
  dout(DBL) << "start_rank ns=" << ns << endl;

  // bind to a socket
  if (accepter.start() < 0) 
	return -1;

  assert(my_rank <= 0);
  if (my_rank == 0) {
	EntityMessenger *m = new_entity(MSG_ADDR_DIRECTORY);
	namer = new Namer(m);
  } 
  else {
	assert(my_rank < 0);
	// connect to namer
	entity_rank[MSG_ADDR_DIRECTORY] = 0;
	rank_addr[0] = ns;
	Sender *sender = connect_rank(0);
	
	// send
	Message *m = new MNSConnect(accepter.listen_addr);
	m->set_dest(MSG_ADDR_DIRECTORY, 0);
	sender->send(m);
	
	// wait
	while (my_rank < 0) 
	  waiting_for_rank.Wait(lock);
	assert(my_rank >= 0);	

	dout(DBL) << "start_rank got rank " << my_rank << endl;
  }
  
  // create rank entity
  messenger = new_entity(MSG_ADDR_RANK(my_rank));
  messenger->set_dispatcher(this);
  messenger->ready();

  lock.Unlock();
  return 0;
}

/* connect_rank
 * NOTE: assumes rank.lock held.
 */
Rank::Sender *Rank::connect_rank(int r)
{
  assert(r >= 0);
  assert(r != rank.my_rank);
  
  dout(DBL) << "connect_rank to " << r << " at " << rank.rank_addr[r] << endl;
  
  // create+connect sender
  Sender *sender = new Sender(rank.rank_addr[r]);
  int rc = sender->connect();
  assert(rc >= 0);

  // start thread.
  sender->create();
  
  // ok!
  rank.rank_sender[r] = sender;
  return sender;
}



void Rank::stop_rank()
{
  accepter.stop();
}



/* lookup
 * NOTE: assumes directory.lock held
 */
void Rank::lookup(msg_addr_t addr)
{
  dout(DBL) << "lookup " << addr << endl;

  assert(looking_up.count(addr) == 0);
  looking_up.insert(addr);

  MNSLookup *r = new MNSLookup(addr);
  messenger->send_message(r, MSG_ADDR_DIRECTORY);
}



/* register_entity 
 */
Rank::EntityMessenger *Rank::register_entity(msg_addr_t addr)
{
  dout(DBL) << "register_entity " << addr << endl;
  lock.Lock();
  
  // register with namer
  if (addr != MSG_ADDR_DIRECTORY) {
	// prepare attempt
	static long reg_attempt = 0;
	long id = ++reg_attempt;
	
	Message *m = new MNSRegister(addr, my_rank, id);
	m->set_dest(MSG_ADDR_DIRECTORY, 0);
	
	// prepare cond
	Cond cond;
	waiting_for_register_cond[id] = &cond;

	// send request
	Sender *sender = rank_sender[0];
	sender->send(m);

	// wait
	while (!waiting_for_register_result.count(id))
	  cond.Wait(lock);
	
	// grab result
	addr = waiting_for_register_result[id];
	dout(DBL) << "register_entity got " << addr << endl;
	
	// clean up
	waiting_for_register_cond.erase(id);
	waiting_for_register_result.erase(id);
  }

  // create messenger
  EntityMessenger *m = new EntityMessenger(addr);

  // add to directory
  entity_rank[addr] = my_rank;
  local[addr] = m;

  lock.Unlock();
  return m;
}

void Rank::unregister_entity(EntityMessenger *msgr)
{
  lock.Lock();
  dout(DBL) << "unregister_entity " << msgr->get_myaddr() << endl;
  
  // remove from local directory.
  assert(local.count(msgr->get_myaddr()));
  local.erase(msgr->get_myaddr());

  // tell nameserver.
  msgr->send_message(new MGenericMessage(MSG_NS_UNREGISTER),
					 MSG_ADDR_DIRECTORY);
  
  lock.Unlock();
}


void Rank::submit_message(Message *m)
{
  const msg_addr_t dest = m->get_dest();

  // lookup
  EntityMessenger *entity = 0;
  Sender *sender = 0;

  lock.Lock();
  {
	if (local.count(dest)) {
	  // local
	  entity = local[dest];
	} else if (entity_rank.count( dest ) &&
			   rank_sender.count( entity_rank[dest] )) {
	  // remote, connected.
	  sender = rank_sender[ entity_rank[dest] ];
	} else {
	  // unknown dest addr.
	  if (looking_up.count(dest) == 0) 
		lookup(dest);
	  waiting_for_lookup[dest].push_back(m);
	}
  }
  lock.Unlock();
  
  // do it
  if (entity) {  
	// local!
	entity->queue_message(m);
  } 
  else if (sender) {
	// remote!
	sender->send(m);
  } 
}




void Rank::dispatch(Message *m) 
{
  lock.Lock();

  dout(DBL) << "dispatching " << *m << endl;

  switch (m->get_type()) {
  case MSG_NS_CONNECTACK:
	handle_connect_ack((MNSConnectAck*)m);
	break;
	
  case MSG_NS_REGISTERACK:
	handle_register_ack((MNSRegisterAck*)m);
	break;
	
  case MSG_NS_LOOKUPREPLY:
	handle_lookup_reply((MNSLookupReply*)m);
	break;
	
  default:
	assert(0);
  }
  
  lock.Unlock();
}

void Rank::handle_connect_ack(MNSConnectAck *m) 
{
  dout(DBL) << "handle_connect_ack, my rank is " << m->get_rank();
  my_rank = m->get_rank();
  waiting_for_rank.SignalAll();
  delete m;

  // logger!
  /*dout(DBL) << "logger" << endl;
  char names[100];
  sprintf(names, "rank%d", my_rank);
  string name = names;
  
  if (g_conf.tcp_log) {
	logger = new Logger(name, (LogType*)&rank_logtype);
	rank_logtype.add_set("num");
	rank_logtype.add_inc("in");
	rank_logtype.add_inc("inb");
	rank_logtype.add_inc("dis");
	rank_logtype.add_set("inq");
	rank_logtype.add_set("inqb");
	rank_logtype.add_set("outq");
	rank_logtype.add_set("outqb");
  }
  */
}


void Rank::handle_register_ack(MNSRegisterAck *m) 
{
  dout(DBL) << "handle_register_ack " << m->get_entity() << endl;

  long tid = m->get_tid();
  waiting_for_register_result[tid] = m->get_entity();
  waiting_for_register_cond[tid]->Signal();
  delete m;
}

void Rank::handle_lookup_reply(MNSLookupReply *m) 
{
  list<Message*> waiting;
  dout(DBL) << "got lookup reply" << endl;
  
  for (map<int,tcpaddr_t>::iterator it = m->rank_addr.begin();
	   it != m->rank_addr.end();
	   it++) {
	dout(DBL) << "lookup got rank " << it->first << " addr " << it->second << endl;

	rank_addr[it->first] = it->second;
	connect_rank(it->first);
  }

  for (map<msg_addr_t, int>::iterator it = m->entity_map.begin();
	   it != m->entity_map.end();
	   it++) {
	msg_addr_t addr = it->first;
	int r = it->second;

	dout(DBL) << "lookup got " << addr << " on rank " << r << endl;
	
	if (r == my_rank) {
	  // deliver locally
	  dout(-DBL) << "delivering lookup results locally" << endl;
	  if (local.count(addr)) {
		local[addr]->queue_messages(waiting_for_lookup[addr]);
		waiting_for_lookup.erase(addr);
	  } else
		lookup(addr);  // try again!
	} else {
	  // take waiters
	  entity_rank[addr] = r;
	  Sender *sender = rank_sender[r];
	  assert(sender);
	  
	  if (waiting_for_lookup.count(addr)) {
		sender->send(waiting_for_lookup[addr]);
		waiting_for_lookup.erase(addr);
	  }
	}
  }

  delete m;
}






/**********************************
 * EntityMessenger
 */

Rank::EntityMessenger::EntityMessenger(msg_addr_t myaddr) :
  Messenger(myaddr),
  stop(false),
  dispatch_thread(this)
{
}

void Rank::EntityMessenger::ready()
{
  dout(DBL) << "ready " << get_myaddr() << endl;

  // start my dispatch thread
  dispatch_thread.create();
}


int Rank::EntityMessenger::shutdown()
{
  dout(DBL) << "shutdown " << get_myaddr() << endl;

  // deregister
  rank.unregister_entity(this);

  // stop dispatch thread
  lock.Lock();
  stop = true;
  cond.Signal();
  lock.Unlock();
  dispatch_thread.join();

  return 0;
}

int Rank::EntityMessenger::send_message(Message *m, msg_addr_t dest, int port, int fromport)
{
  // set envelope
  m->set_source(get_myaddr(), fromport);
  m->set_dest(dest, port);
  m->set_lamport_send_stamp( get_lamport() );
  
  dout(4) << "--> " << m->get_type_name() 
		  << " from " << m->get_source() << ':' << m->get_source_port() 
		  << " to " << m->get_dest() << ':' << m->get_dest_port() 
		  << " ---- " << m 
		  << endl;

  rank.submit_message(m);

  return 0;
}



