
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


#undef dout
#define dout(l)  if (l<=g_conf.debug_ms) cout << "rank" << rank.my_rank << " "



#include "tcp.cc"


Rank rank;


/********************************************
 * Namer
 */

Rank::Namer::Namer(EntityMessenger *msgr) :
  messenger(msgr),
  nrank(0), nclient(0), nmds(0), nosd(0), nmon(0)
{
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
}

Rank::Namer::~Namer()
{
  ::unlink(".ceph_ns");
}


void Rank::Namer::dispatch(Message *m)
{
  rank.lock.Lock();
  int type = m->get_type();
  switch (type) {
  case MSG_NS_CONNECT:
	handle_connect((class MNSConnect*)m);
	break;
  case MSG_NS_REGISTER:
	handle_register((class MNSRegister*)m);
	break;
  case MSG_NS_STARTED:
	handle_started(m);
	break;
  case MSG_NS_UNREGISTER:
	handle_unregister(m);
	break;
  case MSG_NS_LOOKUP:
	handle_lookup((class MNSLookup*)m);
	break;
	
  default:
	assert(0);
  }
  rank.lock.Unlock();
}

void Rank::Namer::handle_connect(MNSConnect *m)
{
  int newrank = nrank++;
  dout(2) << "namer.handle_connect from new rank " << newrank << " " << m->get_addr() << endl;
  
  rank.entity_rank[MSG_ADDR_RANK(newrank)] = newrank;
  rank.entity_unstarted.insert(MSG_ADDR_RANK(newrank));
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
	  
	  /*case MSG_ADDR_RANK_BASE:         // stupid client should be able to figure this out
	  entity = MSG_ADDR_RANK(newrank);
	  break;
	  */

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
  rank.entity_unstarted.insert(entity);

  //++version;
  //update_log[version] = entity;
  
  // reply w/ new id
  messenger->send_message(new MNSRegisterAck(m->get_tid(), entity), 
						  MSG_ADDR_RANK(newrank));

  delete m;
}

void Rank::Namer::handle_started(Message *m)
{
  msg_addr_t who = m->get_source();
  dout(10) << "namer.handle_started from entity " << who << endl;

  assert(rank.entity_unstarted.count(who));
  rank.entity_unstarted.erase(who);

  // anybody waiting?
  if (waiting.count(who)) {
	list<Message*> ls;
	ls.swap(waiting[who]);
	waiting.erase(who);

	dout(10) << "doing waiters on " << who << endl;
	for (list<Message*>::iterator it = ls.begin();
		 it != ls.end();
		 it++) 
	  dispatch(*it);
  }
 
}

void Rank::Namer::handle_unregister(Message *m)
{
  msg_addr_t who = m->get_source();
  dout(2) << "namer.handle_unregister from entity " << who << endl;

  rank.show_dir();
  
  assert(rank.entity_rank.count(who));
  rank.entity_rank.erase(who);

  rank.show_dir();

  // shut myself down?  kick watcher.
  if (rank.entity_rank.size() == 2) {
	dout(10) << "namer.handle_unregister stopping namer" << endl;
	rank.lock.Unlock();
	messenger->shutdown();
	rank.lock.Lock();
  }

  delete m;
}


void Rank::Namer::handle_lookup(MNSLookup *m) 
{
  // have it?
  if (rank.entity_rank.count(m->get_entity()) == 0) {
	dout(10) << "namer " << m->get_source() << " lookup '" << m->get_entity() << "' -> dne" << endl;
	waiting[m->get_entity()].push_back(m);
	return;
  }

  if (rank.entity_unstarted.count(m->get_entity())) {
	dout(10) << "namer " << m->get_source() << " lookup '" << m->get_entity() << "' -> unstarted" << endl;
	waiting[m->get_entity()].push_back(m);
	return;
  }

  // look it up!  
  MNSLookupReply *reply = new MNSLookupReply(m);

  int trank = rank.entity_rank[m->get_entity()];
  reply->entity_rank[m->get_entity()] = trank;
  reply->rank_addr[trank] = rank.rank_addr[trank];

  dout(10) << "namer " << m->get_source() << " lookup '" << m->get_entity() << "' -> rank " << trank << endl;

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
  dout(10) << "accepter.start binding to listen " << endl;
  
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

  //dout(10) << "accepter.start listening on " << myport << endl;
  
  // my address is...
  char host[100];
  gethostname(host, 100);
  //dout(10) << "accepter.start my hostname is " << host << endl;

  struct hostent *myhostname = gethostbyname( host ); 

  struct sockaddr_in my_addr;  
  memset(&my_addr, 0, sizeof(my_addr));

  my_addr.sin_family = myhostname->h_addrtype;
  memcpy((char *) &my_addr.sin_addr.s_addr, 
		 myhostname->h_addr_list[0], 
		 myhostname->h_length);
  my_addr.sin_port = myport;
  
  listen_addr = my_addr;
  
  dout(10) << "accepter.start listen addr is " << listen_addr << endl;

  // start thread
  create();

  return 0;
}

void *Rank::Accepter::entry()
{
  dout(10) << "accepter starting" << endl;

  while (!done) {
	// accept
	struct sockaddr_in addr;
	socklen_t slen = sizeof(addr);
	int sd = ::accept(listen_sd, (sockaddr*)&addr, &slen);
	if (sd > 0) {
	  dout(10) << "accepted incoming on sd " << sd << endl;
	  
	  Receiver *r = new Receiver(sd);
	  r->create();
	  
	  rank.lock.Lock();
	  rank.receivers.insert(r);
	  rank.lock.Unlock();
	} else {
	  dout(10) << "no incoming connection?" << endl;
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
	
	dout(10) << "receiver.entry got message for " << m->get_dest() << endl;

	if (g_conf.ms_single_dispatch) {
	  // submit to single dispatch queue
	  rank.lock.Lock();
	  if (m->get_dest().type() == MSG_ADDR_RANK_BASE) 
		rank.dispatch(m);  
	  else 
		rank._submit_single_dispatch(m);
	  rank.lock.Unlock();
	} else {
	  // find entity
	  EntityMessenger *entity = 0;
	  rank.lock.Lock();
	  {
		if (m->get_dest().type() == MSG_ADDR_RANK_BASE) 
		  rank.dispatch(m);
		else if (rank.local.count(m->get_dest()))
		  entity = rank.local[m->get_dest()];
		else {
		  dout(10) << "got message " << *m << " for " << m->get_dest() << ", which isn't local" << endl;
		  assert(0);
		}
	  }
	  rank.lock.Unlock();
	  
	  if (entity) 
		entity->queue_message(m);		// queue
	}
  }

  // add to reap queue
  rank.lock.Lock();
  rank.receiver_reap_queue.push_back(this);
  rank.wait_cond.Signal();
  rank.lock.Unlock();
  
  return 0;
}

Message *Rank::Receiver::read_message()
{
  // envelope
  //dout(10) << "receiver.read_message from sd " << sd  << endl;
  
  msg_envelope_t env;
  if (!tcp_read( sd, (char*)&env, sizeof(env) ))
	return 0;
  
  if (env.type == 0) {
	dout(10) << "receiver got dummy env, bailing" << endl;
	return 0;
  }

  dout(20) << "receiver got envelope type=" << env.type 
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
	
	dout(DBL+10) << "receiver got frag " << i << " of " << env.nchunks 
				 << " len " << bp.length() << endl;
  }
  
  // unmarshall message
  size_t s = blist.length();
  Message *m = decode_message(env, blist);
  
  dout(20) << "reciever got " << s << " byte message from " 
			   << m->get_source() << endl;
  
  return m;
}


/**************************************
 * Sender
 */

int Rank::Sender::connect()
{
  dout(10) << "sender.connect to " << tcpaddr << endl;

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
	  dout(20) << "sender(to rank" << dest_rank << ") grabbing message(s)" << endl;
	  
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
	dout(20) << "sender sleeping" << endl;
	cond.Wait(lock);
  }
  dout(10) << "sender(to rank" << dest_rank << ") done" << endl;
  
  lock.Unlock();  

  rank.lock.Lock();
  rank.sender_reap_queue.push_back(this);
  rank.wait_cond.Signal();
  rank.lock.Unlock();

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

  dout(20)// << g_clock.now() 
			<< " sending " << m << " " << *m 
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
	dout(10) << "tcp_sending frag " << i << " len " << (*it).length() << endl;
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
  single_dispatcher(this),
  my_rank(r),
  namer(0) {
}
Rank::~Rank()
{
  //FIXME
  if (namer) delete namer;
}


void Rank::_submit_single_dispatch(Message *m)
{
  assert(lock.is_locked());

  if (local.count(m->get_dest()) &&
	  local[m->get_dest()]->is_ready()) {
	rank.single_dispatch_queue.push_back(m);
	rank.single_dispatch_cond.Signal();
  } else {
	waiting_for_ready[m->get_dest()].push_back(m);
  }
}


void Rank::single_dispatcher_entry()
{
  lock.Lock();
  while (!single_dispatch_stop || !single_dispatch_queue.empty()) {
	if (!single_dispatch_queue.empty()) {
	  list<Message*> ls;
	  ls.swap(single_dispatch_queue);

	  lock.Unlock();
	  {
		while (!ls.empty()) {
		  Message *m = ls.front();
		  ls.pop_front();
		  
		  dout(1) //<< g_clock.now() 
				  << "---- " << m->get_type_name() 
				  << " from " << m->get_source() << ':' << m->get_source_port() 
				  << " to " << m->get_dest() << ':' << m->get_dest_port()
				  << " ---- " << m 
				  << endl;
		  
		  assert(local.count(m->get_dest()));
		  local[m->get_dest()]->dispatch(m);
		}
	  }
	  lock.Lock();
	  continue;
	}
	single_dispatch_cond.Wait(lock);
  }
  lock.Unlock();
}


/*
 * note: assumes lock is held
 */
void Rank::reaper()
{
  assert(lock.is_locked());

  while (!receiver_reap_queue.empty()) {
	Receiver *r = receiver_reap_queue.front();
	receiver_reap_queue.pop_front();
	//dout(10) << "reaper reaping receiver sd " << r->sd << endl;
	receivers.erase(r);
	r->join();
	delete r;
	dout(10) << "reaper reaped receiver sd " << r->sd << endl;
  }

  while (!sender_reap_queue.empty()) {
	Sender *s = sender_reap_queue.front();
	sender_reap_queue.pop_front();
	//dout(10) << "reaper reaping sender rank " << s->dest_rank << " at " << s->tcpaddr << endl;
	if (rank_sender.count(s->dest_rank) &&
		rank_sender[s->dest_rank] == s)
	  rank_sender.erase(s->dest_rank);
	s->join();
	delete s;
	dout(10) << "reaper reaped sender rank " << s->dest_rank << " at " << s->tcpaddr << endl;
  }
}


int Rank::start_rank(tcpaddr_t& ns)
{
  dout(10) << "start_rank ns=" << ns << endl;

  // bind to a socket
  if (accepter.start() < 0) 
	return -1;

  // start single thread dispatcher?
  if (g_conf.ms_single_dispatch) {
	single_dispatch_stop = false;
	single_dispatcher.create();
  }	

  lock.Lock();

  assert(my_rank <= 0);
  if (my_rank == 0) {
	rank_addr[0] = accepter.listen_addr;

	// create namer0
	msg_addr_t naddr = MSG_ADDR_NAMER(0);
	entity_rank[naddr] = 0;
	local[naddr] = new EntityMessenger(naddr);
	namer = new Namer(local[naddr]);

	// create rank0
	msg_addr_t raddr = MSG_ADDR_RANK(0);
	entity_rank[raddr] = 0;
	entity_unstarted.insert(raddr);
	local[raddr] = messenger = new EntityMessenger(raddr);
	messenger->set_dispatcher(this);
  } 
  else {
	assert(my_rank < 0);
	dout(10) << "start_rank connecting to namer0" << endl;
	
	// connect to namer
	entity_rank[MSG_ADDR_NAMER(0)] = 0;
	rank_addr[0] = ns;
	Sender *sender = connect_rank(0);
	
	// send
	Message *m = new MNSConnect(accepter.listen_addr);
	m->set_dest(MSG_ADDR_NAMER(0), 0);
	sender->send(m);
	
	// wait
	while (my_rank < 0) 
	  waiting_for_rank.Wait(lock);
	assert(my_rank >= 0);	
	
	dout(10) << "start_rank got rank " << my_rank << endl;
    
	// create rank entity
	entity_rank[MSG_ADDR_RANK(my_rank)] = my_rank;
	local[MSG_ADDR_RANK(my_rank)] = messenger = new EntityMessenger(MSG_ADDR_RANK(my_rank));
	messenger->set_dispatcher(this);
  }

  lock.Unlock();
  return 0;
}

/* connect_rank
 * NOTE: assumes rank.lock held.
 */
Rank::Sender *Rank::connect_rank(int r)
{
  assert(rank.lock.is_locked());
  assert(r >= 0);
  assert(r != rank.my_rank);
  
  dout(10) << "connect_rank to " << r << " at " << rank.rank_addr[r] << endl;
  
  // create+connect sender
  Sender *sender = new Sender(r, rank.rank_addr[r]);
  int rc = sender->connect();
  assert(rc >= 0);

  // old sender?
  if (rank.rank_sender.count(r))
	rank.rank_sender[r]->stop();
  
  // start thread.
  sender->create();

  // ok!
  rank.rank_sender[r] = sender;
  return sender;
}





void Rank::show_dir()
{
  dout(10) << "show_dir ---" << endl;
  
  for (map<msg_addr_t, int>::iterator i = entity_rank.begin();
	   i != entity_rank.end();
	   i++) {
	if (local.count(i->first)) {
	  dout(10) << "show_dir entity_rank " << i->first << " -> rank" << i->second << " local " << endl;
	} else {
	  dout(10) << "show_dir entity_rank " << i->first << " -> rank" << i->second << endl;
	}
  }
}


/* lookup
 * NOTE: assumes directory.lock held
 */
void Rank::lookup(msg_addr_t addr)
{
  dout(10) << "lookup " << addr << endl;
  assert(lock.is_locked());

  assert(looking_up.count(addr) == 0);
  looking_up.insert(addr);

  MNSLookup *r = new MNSLookup(addr);
  messenger->send_message(r, MSG_ADDR_DIRECTORY);
}



/* register_entity 
 */
Rank::EntityMessenger *Rank::register_entity(msg_addr_t addr)
{
  dout(10) << "register_entity " << addr << endl;
  lock.Lock();
  
  // register with namer
  static long reg_attempt = 0;
  long id = ++reg_attempt;
  
  Message *reg = new MNSRegister(addr, my_rank, id);
  reg->set_dest(MSG_ADDR_DIRECTORY, 0);
  
  // prepare cond
  Cond cond;
  waiting_for_register_cond[id] = &cond;
  
  // send request
  lock.Unlock();
  submit_message(reg);
  lock.Lock();
  
  // wait
  while (!waiting_for_register_result.count(id))
	cond.Wait(lock);
  
  // grab result
  addr = waiting_for_register_result[id];
  dout(10) << "register_entity got " << addr << endl;
  
  // clean up
  waiting_for_register_cond.erase(id);
  waiting_for_register_result.erase(id);
  
  // create messenger
  EntityMessenger *msgr = new EntityMessenger(addr);

  // add to directory
  entity_rank[addr] = my_rank;
  local[addr] = msgr;

  lock.Unlock();
  return msgr;
}

void Rank::unregister_entity(EntityMessenger *msgr)
{
  lock.Lock();
  dout(10) << "unregister_entity " << msgr->get_myaddr() << endl;
  
  // remove from local directory.
  assert(local.count(msgr->get_myaddr()));
  local.erase(msgr->get_myaddr());

  if (my_rank > 0) {
	assert(entity_rank.count(msgr->get_myaddr()));
	entity_rank.erase(msgr->get_myaddr());
  } // else namer will do it.

  // tell namer.
  if (msgr->get_myaddr() != MSG_ADDR_NAMER(0) &&
	  msgr->get_myaddr() != MSG_ADDR_RANK(0))
	msgr->send_message(new MGenericMessage(MSG_NS_UNREGISTER),
					   MSG_ADDR_DIRECTORY);
  
  // kick wait()?
  if (local.size() <= 2)
	wait_cond.Signal();   

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
	  if (g_conf.ms_single_dispatch) {
		_submit_single_dispatch(m);
	  } else {
		entity = local[dest];
	  }
	} else if (entity_rank.count( dest )) {
	  // remote.
	  if (rank_sender.count( entity_rank[dest] )) {
		// connected.
		sender = rank_sender[ entity_rank[dest] ];
	  } else {
		// not connected.
		sender = connect_rank( entity_rank[dest] );
	  }
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

  dout(10) << "dispatching " << *m << endl;

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
  dout(10) << "handle_connect_ack, my rank is " << m->get_rank();
  my_rank = m->get_rank();
  waiting_for_rank.SignalAll();
  delete m;

  // logger!
  /*dout(10) << "logger" << endl;
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
  dout(10) << "handle_register_ack " << m->get_entity() << endl;

  long tid = m->get_tid();
  waiting_for_register_result[tid] = m->get_entity();
  waiting_for_register_cond[tid]->Signal();
  delete m;
}

void Rank::handle_lookup_reply(MNSLookupReply *m) 
{
  list<Message*> waiting;
  dout(10) << "got lookup reply" << endl;
  
  for (map<int,tcpaddr_t>::iterator it = m->rank_addr.begin();
	   it != m->rank_addr.end();
	   it++) {
	dout(10) << "lookup got rank " << it->first << " addr " << it->second << endl;

	rank_addr[it->first] = it->second;
	connect_rank(it->first);
  }

  for (map<msg_addr_t, int>::iterator it = m->entity_rank.begin();
	   it != m->entity_rank.end();
	   it++) {
	msg_addr_t addr = it->first;
	int r = it->second;

	dout(10) << "lookup got " << addr << " on rank " << r << endl;
	
	if (r == my_rank) {
	  // deliver locally
	  dout(-DBL) << "delivering lookup results locally" << endl;
	  if (local.count(addr)) {
		if (g_conf.ms_single_dispatch) {
		  single_dispatch_queue.splice(single_dispatch_queue.end(),
									   waiting_for_lookup[addr]);
		} else {
		  local[addr]->queue_messages(waiting_for_lookup[addr]);
		}
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


void Rank::wait()
{
  lock.Lock();
  while (1) {
	// reap dead senders, receivers.
	reaper();

	if (local.size() == 0) {
	  dout(10) << "wait: everything stopped" << endl;
	  break;   // everything stopped.
	}

	if (local.size() == 1 &&
		!messenger->is_stopped()) {
	  dout(10) << "wait: stopping rank" << endl;
	  lock.Unlock();
	  messenger->shutdown();
	  lock.Lock();
	  continue;
	}

	wait_cond.Wait(lock);
  }
  lock.Unlock();

  // stop dispatch thread
  if (g_conf.ms_single_dispatch) {
	dout(10) << "wait: stopping dispatch thread" << endl;
	lock.Lock();
	single_dispatch_stop = true;
	single_dispatch_cond.Signal();
	lock.Unlock();
	single_dispatcher.join();
  }

  // reap senders and receivers
  lock.Lock();
  {
	dout(10) << "wait: stopping senders" << endl;
	for (map<int,Sender*>::iterator i = rank_sender.begin();
		 i != rank_sender.end();
		 i++)
	  i->second->stop();
	while (!rank_sender.empty()) {
	  wait_cond.Wait(lock);
	  reaper();
	}

	if (0) {  // stop() no worky
	  dout(10) << "wait: stopping receivers" << endl;
	  for (set<Receiver*>::iterator i = receivers.begin();
		   i != receivers.end();
		   i++) 
		(*i)->stop();
	  while (!receivers.empty()) {
		wait_cond.Wait(lock);
		reaper();
	  }	  
	}

  }
  lock.Unlock();

  dout(10) << "wait: done." << endl;
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
Rank::EntityMessenger::~EntityMessenger()
{
}

void Rank::EntityMessenger::dispatch_entry()
{
  lock.Lock();
  while (!stop) {
	if (!dispatch_queue.empty()) {
	  list<Message*> ls;
	  ls.swap(dispatch_queue);

	  lock.Unlock();
	  {
		// deliver
		while (!ls.empty()) {
		  Message *m = ls.front();
		  ls.pop_front();
		  dout(1) //<< g_clock.now()
				  << "---- " << m->get_type_name() 
				  << " from " << m->get_source() << ':' << m->get_source_port() 
				  << " to " << m->get_dest() << ':' << m->get_dest_port()
				  << " ---- " << m 
				  << endl;
		  dispatch(m);
		}
	  }
	  lock.Lock();
	  continue;
	}
	cond.Wait(lock);
  }
  lock.Unlock();
}

void Rank::EntityMessenger::ready()
{
  dout(10) << "ready " << get_myaddr() << endl;

  if (g_conf.ms_single_dispatch) {
	rank.lock.Lock();
	if (rank.waiting_for_ready.count(get_myaddr())) {
	  rank.single_dispatch_queue.splice(rank.single_dispatch_queue.end(),
										rank.waiting_for_ready[get_myaddr()]);
	  rank.waiting_for_ready.erase(get_myaddr());
	  rank.single_dispatch_cond.Signal();
	}
	rank.lock.Unlock();
  } else {
	// start my dispatch thread
	dispatch_thread.create();
  }

  // tell namer
  if (get_myaddr() != MSG_ADDR_NAMER(0))
	send_message(new MGenericMessage(MSG_NS_STARTED), MSG_ADDR_NAMER(0));
}


int Rank::EntityMessenger::shutdown()
{
  dout(10) << "shutdown " << get_myaddr() << endl;

  // deregister
  rank.unregister_entity(this);

  // stop my dispatch thread
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
  
  dout(1) << "--> " << m->get_type_name() 
		  << " from " << m->get_source() << ':' << m->get_source_port() 
		  << " to " << m->get_dest() << ':' << m->get_dest_port() 
		  << " ---- " << m 
		  << endl;

  rank.submit_message(m);

  return 0;
}



