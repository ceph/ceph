
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
#include "messages/MNSFailure.h"

//#include "messages/MFailure.h"

#include <netdb.h>


#undef dout
#define dout(l)  if (l<=g_conf.debug_ms) cout << g_clock.now() << " -- rank" << rank.my_rank << " "
#define derr(l)  if (l<=g_conf.debug_ms) cerr << g_clock.now() << " -- rank" << rank.my_rank << " "



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
  nrank = g_conf.num_mon;
  
  // announce myself
  /*
  cerr << "ceph ns is " << rank.accepter.listen_addr << endl;
  cout << "export CEPH_NAMESERVER=" << rank.accepter.listen_addr << endl;
  int fd = ::open(".ceph_ns", O_WRONLY|O_CREAT);
  ::write(fd, (void*)&rank.accepter.listen_addr, sizeof(tcpaddr_t));
  ::fchmod(fd, 0755);
  ::close(fd);
  */

  // ok
  messenger->set_dispatcher(this);
}

Rank::Namer::~Namer()
{
  //::unlink(".ceph_ns");
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
  case MSG_NS_FAILURE:
    handle_failure((class MNSFailure*)m);
    break;
    
  case MSG_FAILURE_ACK:
    delete m;
    break;

  default:
    assert(0);
  }
  rank.lock.Unlock();
}

void Rank::Namer::handle_connect(MNSConnect *m)
{
  int newrank = nrank++;
  dout(2) << "namer.handle_connect from new rank" << newrank << " " << m->get_addr() << endl;
  
  rank.entity_map[MSG_ADDR_RANK(newrank)].addr = m->get_addr();
  rank.entity_map[MSG_ADDR_RANK(newrank)].rank = newrank;
  rank.entity_unstarted.insert(MSG_ADDR_RANK(newrank));

  messenger->send_message(new MNSConnectAck(newrank),
                          MSG_ADDR_RANK(newrank), rank.entity_map[MSG_ADDR_RANK(newrank)]);
  delete m;
}

void Rank::Namer::manual_insert_inst(const entity_inst_t &inst)
{
  rank.entity_map[MSG_ADDR_RANK(inst.rank)] = inst;
}

void Rank::Namer::handle_register(MNSRegister *m)
{
  dout(10) << "namer.handle_register from rank " << m->get_rank()
          << " addr " << m->get_entity() << endl;
  
  // pick id
  entity_name_t entity = m->get_entity();

  if (entity.is_new()) {
    // make up a new address!
    switch (entity.type()) {
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


  // register
  if (rank.entity_map.count(entity)) {
    dout(1) << "namer.handle_register re-registering " << entity
            << " inst " << m->get_source_inst()
            << " (was " << rank.entity_map[entity] << ")"
            << endl;
  } else {
    dout(1) << "namer.handle_register registering " << entity
            << " inst " << m->get_source_inst()
            << endl;
  }
  rank.entity_map[entity] = m->get_source_inst();
  rank.entity_unstarted.insert(entity);
  
  // reply w/ new id
  messenger->send_message(new MNSRegisterAck(m->get_tid(), entity), 
                          m->get_source(), rank.entity_map[entity]);
  
  delete m;
}

void Rank::Namer::handle_started(Message *m)
{
  entity_name_t who = m->get_source();
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
  entity_name_t who = m->get_source();
  dout(1) << "namer.handle_unregister entity " << who << endl;

  rank.show_dir();
  
  assert(rank.entity_map.count(who));
  rank.entity_map.erase(who);

  rank.show_dir();

  // shut myself down?  kick watcher.
  if (rank.entity_map.size() == 2) {
    dout(10) << "namer.handle_unregister stopping namer" << endl;
    rank.lock.Unlock();
    messenger->shutdown();
    delete messenger;
    rank.lock.Lock();
  }

  delete m;
}


void Rank::Namer::handle_lookup(MNSLookup *m) 
{
  // have it?
  if (rank.entity_map.count(m->get_entity()) == 0) {
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

  reply->entity_map[m->get_entity()] = rank.entity_map[m->get_entity()];

  dout(10) << "namer " << m->get_source()
           << " lookup '" << m->get_entity() 
           << "' -> " << rank.entity_map[m->get_entity()] << endl;
  
  messenger->send_message(reply, m->get_source(), m->get_source_inst());
  delete m;
}

void Rank::Namer::handle_failure(MNSFailure *m)
{
  dout(10) << "namer.handle_failure inst " << m->get_inst()
           << endl;

  // search for entities on this instance
  list<entity_name_t> rm;
  for (hash_map<entity_name_t,entity_inst_t>::iterator i = rank.entity_map.begin();
       i != rank.entity_map.end();
       i++) {
    if (i->second != m->get_inst()) continue;
    rm.push_back(i->first);
  }
  for (list<entity_name_t>::iterator i = rm.begin();
       i != rm.end();
       i++) {
    dout(10) << "namer.handle_failure inst " << m->get_inst()
             << ", removing " << *i << endl;
    
    rank.entity_map.erase(*i);
    rank.entity_unstarted.erase(*i);
    
    /*
    if ((*i).is_osd()) {
      // tell the monitor
      messenger->send_message(new MFailure(*i, m->get_inst()), MSG_ADDR_MON(0));
    }
    */
  }

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
  bzero(host, 100);
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

    EntityMessenger *entity = 0;

    rank.lock.Lock();
    {
      if (rank.down.count(m->get_dest())) {
        dout(0) << "receiver.entry dest " << m->get_dest() << " down, dropping " << *m << endl;
        delete m;

        if (rank.looking_up.count(m->get_dest()) == 0) 
          rank.lookup(m->get_dest());
      }
      else if (rank.entity_map.count(m->get_source()) &&
               rank.entity_map[m->get_source()] > m->get_source_inst()) {
        derr(0) << "receiver.entry source " << m->get_source() 
                << " inst " << m->get_source_inst() 
                << " < " << rank.entity_map[m->get_source()] 
                << ", dropping " << *m << endl;
        delete m;
      }
      else {
        if (rank.entity_map.count(m->get_source()) &&
            rank.entity_map[m->get_source()] > m->get_source_inst()) {
          derr(0) << "receiver.entry source " << m->get_source() 
                  << " inst " << m->get_source_inst() 
                  << " > " << rank.entity_map[m->get_source()] 
                  << ", WATCH OUT " << *m << endl;
          rank.entity_map[m->get_source()] = m->get_source_inst();
        }

        if (m->get_dest().type() == MSG_ADDR_RANK_BASE) {
          // ours.
          rank.dispatch(m);
        } else {
          if (g_conf.ms_single_dispatch) {
            // submit to single dispatch queue
            rank._submit_single_dispatch(m);
          } else {
            if (rank.local.count(m->get_dest())) {
              // find entity
              entity = rank.local[m->get_dest()];
            } else {
              derr(0) << "got message " << *m << " for " << m->get_dest() << ", which isn't local" << endl;
              rank.waiting_for_lookup[m->get_dest()].push_back(m);
            }
          }
        }
      }
    }
    rank.lock.Unlock();
      
    if (entity) 
      entity->queue_message(m);        // queue
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
    
    dout(20) << "receiver got frag " << i << " of " << env.nchunks 
                 << " len " << bp.length() << endl;
  }
  
  // unmarshall message
  size_t s = blist.length();
  Message *m = decode_message(env, blist);
  
  dout(20) << "receiver got " << s << " byte message from " 
               << m->get_source() << endl;
  
  return m;
}


/**************************************
 * Sender
 */

int Rank::Sender::connect()
{
  dout(10) << "sender(" << inst << ").connect" << endl;

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
  int r = ::connect(sd, (sockaddr*)&inst.addr, sizeof(myAddr));
  if (r < 0) return r;

  // identify myself
  // FIXME
  
  return 0;
}


void Rank::Sender::finish()
{
  dout(10) << "sender(" << inst << ").finish" << endl;

  // make sure i get reaped.
  rank.lock.Lock();
  rank.sender_reap_queue.push_back(this);
  rank.wait_cond.Signal();
  rank.lock.Unlock();
}

void Rank::Sender::fail_and_requeue(list<Message*>& out)
{
  dout(10) << "sender(" << inst << ").fail" << endl;// and requeue" << endl;

  // tell namer
  if (!rank.messenger) {
    derr(0) << "FATAL error: can't send failure to namer0, not connected yet" << endl;
    assert(0);
  }

  // old and unnecessary?
  if (0)
    rank.messenger->send_message(new MNSFailure(inst),
                                 MSG_ADDR_NAMER(0));


  // FIXME: possible race before i reclaim lock here?
  
  Dispatcher *dis = 0;
  entity_name_t dis_dest;
  
  list<Message*> lost;

  // requeue my messages
  rank.lock.Lock();
  lock.Lock();
  {
    // include out at front of queue
    q.splice(q.begin(), out);  
    dout(10) << "sender(" << inst << ").fail " 
             << q.size() << " messages" << endl;
    
    if (0) {
      lost.swap(q);
    } else {

      while (!q.empty()) {
        // don't keep reconnecting..
        if (rank.entity_map.count(q.front()->get_dest()) &&
            rank.entity_map[q.front()->get_dest()] == inst)
          rank.down.insert(q.front()->get_dest());
        //rank.entity_map.erase(q.front()->get_dest());
        
        if (!dis &&
            rank.local.count(q.front()->get_source())) {
          dis_dest = q.front()->get_dest();
          dis = rank.local[q.front()->get_source()]->get_dispatcher();
        }
        
        if (g_conf.ms_requeue_on_sender_fail)
          rank.submit_message( q.front() );
        else
          lost.push_back( q.front() );
        q.pop_front();
      }
    }

    // deactivate myself
    if (rank.rank_sender.count(inst.rank) &&
        rank.rank_sender[inst.rank] == this)
      rank.rank_sender.erase(inst.rank);

    // stop sender loop
    done = true;
  }
  lock.Unlock();


  // send special failure msg?
  if (dis) {
    for (list<Message*>::iterator p = lost.begin();
         p != lost.end();
         p++)
      dis->ms_handle_failure(*p, dis_dest, inst);
  }
  
  rank.lock.Unlock();
}

void *Rank::Sender::entry()
{
  // connect
  if (sd == 0) {
	int rc = connect();
	if (rc < 0) {
	  list<Message*> out;
	  derr(0) << "error connecting to " << inst << endl;
	  fail_and_requeue(out);
	  finish();
	  return 0;
	}
  }

  lock.Lock();
  while (!q.empty() || !done) {
    
    if (!q.empty()) {
      dout(20) << "sender(" << inst << ") grabbing message(s)" << endl;
      
      // grab outgoing list
      list<Message*> out;
      out.swap(q);
      
      // drop lock while i send these
      lock.Unlock();
      
      while (!out.empty()) {
        Message *m = out.front();
        out.pop_front();

        dout(20) << "sender(" << inst << ") sending " << *m << endl;

        // stamp.
        m->set_source_inst(rank.my_inst);
        
        // marshall
        if (m->empty_payload())
          m->encode_payload();
        
        if (write_message(m) < 0) {
          // failed!
          derr(0) << "error sending to " << m->get_dest() << " on " << inst << endl;
          out.push_front(m);
          fail_and_requeue(out);
          break;
        }
      }

      lock.Lock();
      continue;
    }
    
    // wait
    dout(20) << "sender(" << inst << ") sleeping" << endl;
    cond.Wait(lock);
  }
  lock.Unlock(); 
  
  finish();
  return 0;
}


int Rank::Sender::write_message(Message *m)
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
    derr(20) << "error sending envelope for " << *m
             << " to " << m->get_dest() << endl; 
    return -1;
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
    if (r < 0) { 
      derr(20) << "error sending chunk len for " << *m << " to " << m->get_dest() << endl; 
      return -1;
    }
    r = tcp_write( sd, (*it).c_str(), size );
    if (r < 0) { 
      derr(20) << "error sending data chunk for " << *m << " to " << m->get_dest() << endl; 
      return -1;
    }
    i++;
  }
#else
  // one big chunk
  int size = blist.length();
  r = tcp_write( sd, (char*)&size, sizeof(size) );
  if (r < 0) { 
    derr(20) << "error sending data len for " << *m << " to " << m->get_dest() << endl; 
    return -1;
  }
  for (list<bufferptr>::iterator it = blist.buffers().begin();
       it != blist.buffers().end();
       it++) {
    r = tcp_write( sd, (*it).c_str(), (*it).length() );
    if (r < 0) { 
      derr(20) << "error sending data megachunk for " << *m << " to " << m->get_dest() << " : len " << (*it).length() << endl; 
      return -1;
    }
  }
#endif
  
  // delete message
  delete m;
  return 0;
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
                  << "---- " 
                  << m->get_source() << ':' << m->get_source_port() 
                  << " to " << m->get_dest() << ':' << m->get_dest_port()
                  << " ---- " << m->get_type_name() 
                  << " ---- " << m 
                  << endl;
          
          if (m->get_dest().type() == MSG_ADDR_RANK_BASE)
            rank.dispatch(m);
          else {
            assert(local.count(m->get_dest()));
            local[m->get_dest()]->dispatch(m);
          }
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
    dout(10) << "reaper reaped receiver sd " << r->sd << endl;
    delete r;
  }

  while (!sender_reap_queue.empty()) {
    Sender *s = sender_reap_queue.front();
    sender_reap_queue.pop_front();
    //dout(10) << "reaper reaping sender rank " << s->dest_rank << " at " << s->tcpaddr << endl;
    if (rank_sender.count(s->inst.rank) &&
        rank_sender[s->inst.rank] == s)
      rank_sender.erase(s->inst.rank);
    s->join();
    dout(10) << "reaper reaped sender " << s->inst << endl;
    delete s;
  }
}


int Rank::start_rank()
{
  dout(10) << "start_rank" << endl;

  // bind to a socket
  if (accepter.start() < 0) 
    return -1;

  // start single thread dispatcher?
  if (g_conf.ms_single_dispatch) {
    single_dispatch_stop = false;
    single_dispatcher.create();
  }    

  lock.Lock();

  if (my_rank < 0) {
    dout(10) << "start_rank connecting to namer0" << endl;
    
    // connect to namer
    assert(entity_map.count(MSG_ADDR_NAMER(0)));
    Sender *sender = connect_rank(entity_map[MSG_ADDR_NAMER(0)]);
    
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
    entity_map[MSG_ADDR_RANK(my_rank)] = my_inst;
    local[MSG_ADDR_RANK(my_rank)] = messenger = new EntityMessenger(MSG_ADDR_RANK(my_rank));
    messenger->set_dispatcher(this);
  } else {
    // my_inst
    my_inst.addr = accepter.listen_addr;
    my_inst.rank = my_rank;

    // create my rank
    entity_name_t raddr = MSG_ADDR_RANK(my_rank);
    entity_map[raddr] = my_inst;
    entity_unstarted.insert(raddr);
    local[raddr] = messenger = new EntityMessenger(raddr);
    messenger->set_dispatcher(this);
    
    dout(1) << "start_rank " << my_rank << " at " << my_inst << endl;
  } 

  lock.Unlock();
  return 0;
}

void Rank::start_namer()
{
  // create namer0
  entity_name_t naddr = MSG_ADDR_NAMER(0);
  entity_map[naddr] = my_inst;
  local[naddr] = new EntityMessenger(naddr);
  namer = new Namer(local[naddr]);
}

void Rank::set_namer(const tcpaddr_t& ns)
{
  entity_map[MSG_ADDR_NAMER(0)].addr = ns;
  entity_map[MSG_ADDR_NAMER(0)].rank = 0;
}

/* connect_rank
 * NOTE: assumes rank.lock held.
 */
Rank::Sender *Rank::connect_rank(const entity_inst_t& inst)
{
  assert(rank.lock.is_locked());
  assert(inst != rank.my_inst);
  
  dout(10) << "connect_rank to " << inst << endl;
  
  // create sender
  Sender *sender = new Sender(inst);
  //int rc = sender->connect();
  //assert(rc >= 0);

  // start thread.
  sender->create();

  // old sender?
  assert(rank.rank_sender.count(inst.rank) == 0);
  //if (rank.rank_sender.count(r))
  //rank.rank_sender[r]->stop();  

  // ok!
  rank.rank_sender[inst.rank] = sender;
  return sender;
}





void Rank::show_dir()
{
  dout(10) << "show_dir ---" << endl;
  
  for (hash_map<entity_name_t, entity_inst_t>::iterator i = entity_map.begin();
       i != entity_map.end();
       i++) {
    if (local.count(i->first)) {
      dout(10) << "show_dir entity_map " << i->first << " -> " << i->second << " local " << endl;
    } else {
      dout(10) << "show_dir entity_map " << i->first << " -> " << i->second << endl;
    }
  }
}


/* lookup
 * NOTE: assumes directory.lock held
 */
void Rank::lookup(entity_name_t addr)
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
Rank::EntityMessenger *Rank::register_entity(entity_name_t addr)
{
  dout(10) << "register_entity " << addr << endl;
  lock.Lock();
  
  // register with namer
  static long reg_attempt = 0;
  long id = ++reg_attempt;
  
  Message *reg = new MNSRegister(addr, my_rank, id);
  reg->set_source(MSG_ADDR_RANK(my_rank), 0);
  reg->set_source_inst(my_inst);
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
  entity_map[addr] = my_inst;
  local[addr] = msgr;

  // was anyone waiting?
  if (waiting_for_lookup.count(addr)) {
    submit_messages(waiting_for_lookup[addr]);
    waiting_for_lookup.erase(addr);
  }

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
    assert(entity_map.count(msgr->get_myaddr()));
    entity_map.erase(msgr->get_myaddr());
  } // else namer will do it.

  // tell namer.
  if (msgr->get_myaddr() != MSG_ADDR_NAMER(0) &&
      msgr->get_myaddr() != MSG_ADDR_RANK(0))
    msgr->send_message(new MGenericMessage(MSG_NS_UNREGISTER),
                       MSG_ADDR_NAMER(0));
  
  // kick wait()?
  if (local.size() <= 2)
    wait_cond.Signal();   

  lock.Unlock();
}


void Rank::submit_messages(list<Message*>& ls)
{
  for (list<Message*>::iterator i = ls.begin(); i != ls.end(); i++)
    submit_message(*i);
  ls.clear();
}


void Rank::prepare_dest(entity_name_t dest)
{
  lock.Lock();

  if (entity_map.count( dest )) {
    // remote, known rank addr.
    entity_inst_t inst = entity_map[dest];
    
    if (inst == my_inst) {
      //dout(20) << "submit_message " << *m << " dest " << dest << " local but mid-register, waiting." << endl;
      //waiting_for_lookup[dest].push_back(m);
    }
    else if (rank_sender.count( inst.rank ) &&
             rank_sender[inst.rank]->inst == inst) {
      //dout(20) << "submit_message " << *m << " dest " << dest << " remote, " << inst << ", connected." << endl;
      // connected.
      //sender = rank_sender[ inst.rank ];
    } else {
      //dout(20) << "submit_message " << *m << " dest " << dest << " remote, " << inst << ", connecting." << endl;
      // not connected.
      connect_rank( inst );
    }
  } else {
    // unknown dest rank or rank addr.
    if (looking_up.count(dest) == 0) {
      //dout(20) << "submit_message " << *m << " dest " << dest << " remote, unknown addr, looking up" << endl;
      lookup(dest);
    } else {
      //dout(20) << "submit_message " << *m << " dest " << dest << " remote, unknown addr, already looking up" << endl;
    }
  }

  lock.Unlock();
}

void Rank::submit_message(Message *m, const entity_inst_t& dest_inst)
{
  const entity_name_t dest = m->get_dest();

  // lookup
  EntityMessenger *entity = 0;
  Sender *sender = 0;

  lock.Lock();
  {
    // local?
    if (dest_inst.rank == my_inst.rank) {
      if (local.count(dest)) {
        // local
        dout(20) << "submit_message " << *m << " dest " << dest << " local" << endl;
        if (g_conf.ms_single_dispatch) {
          _submit_single_dispatch(m);
        } else {
          entity = local[dest];
        }
      } else {
        // mid-register
        dout(20) << "submit_message " << *m << " dest " << dest << " local but mid-register, waiting." << endl;
        assert(0);
        waiting_for_lookup[dest].push_back(m);
      }
    }
    else {
      // remote.
      if (rank_sender.count( dest_inst.rank )) {
        //&&
        //rank_sender[dest_inst.rank]->inst == dest_inst) {
        dout(20) << "submit_message " << *m << " dest " << dest << " remote, " << dest_inst << ", connected." << endl;
        // connected.
        sender = rank_sender[ dest_inst.rank ];
      } else {
        dout(20) << "submit_message " << *m << " dest " << dest << " remote, " << dest_inst << ", connecting." << endl;
        // not connected.
        sender = connect_rank( dest_inst );
      }
    }
  }
  lock.Unlock();
  
  // do it
  if (entity) {  
    // local!
    dout(20) << "submit_message " << *m << " dest " << dest << " local, queueing" << endl;
    entity->queue_message(m);
  } 
  else if (sender) {
    // remote!
    dout(20) << "submit_message " << *m << " dest " << dest << " remote, sending" << endl;
    sender->send(m);
  } 
}


void Rank::submit_message(Message *m)
{
  const entity_name_t dest = m->get_dest();

  // lookup
  EntityMessenger *entity = 0;
  Sender *sender = 0;

  lock.Lock();
  {
    if (down.count(dest)) {
      // black hole.
      dout(0) << "submit_message " << *m << " dest " << dest << " down, dropping" << endl;
      delete m;

      if (looking_up.count(dest) == 0) 
        lookup(dest);

    } else if (local.count(dest)) {
      dout(20) << "submit_message " << *m << " dest " << dest << " local" << endl;

      // local
      if (g_conf.ms_single_dispatch) {
        _submit_single_dispatch(m);
      } else {
        entity = local[dest];
      }
    } else if (entity_map.count( dest )) {
      // remote, known rank addr.
      entity_inst_t inst = entity_map[dest];

      if (inst == my_inst) {
        dout(20) << "submit_message " << *m << " dest " << dest << " local but mid-register, waiting." << endl;
        waiting_for_lookup[dest].push_back(m);
      }
      else if (rank_sender.count( inst.rank ) &&
          rank_sender[inst.rank]->inst == inst) {
        dout(20) << "submit_message " << *m << " dest " << dest << " remote, " << inst << ", connected." << endl;
        // connected.
        sender = rank_sender[ inst.rank ];
      } else {
        dout(20) << "submit_message " << *m << " dest " << dest << " remote, " << inst << ", connecting." << endl;
        // not connected.
        sender = connect_rank( inst );
      }
    } else {
      // unknown dest rank or rank addr.
      if (looking_up.count(dest) == 0) {
        dout(20) << "submit_message " << *m << " dest " << dest << " remote, unknown addr, looking up" << endl;
        lookup(dest);
      } else {
        dout(20) << "submit_message " << *m << " dest " << dest << " remote, unknown addr, already looking up" << endl;
      }
      waiting_for_lookup[dest].push_back(m);
    }
  }
  lock.Unlock();
  
  // do it
  if (entity) {  
    // local!
    dout(20) << "submit_message " << *m << " dest " << dest << " local, queueing" << endl;
    entity->queue_message(m);
  } 
  else if (sender) {
    // remote!
    dout(20) << "submit_message " << *m << " dest " << dest << " remote, sending" << endl;
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
  dout(10) << "handle_connect_ack, my rank is " << m->get_rank() << endl;
  my_rank = m->get_rank();

  my_inst.addr = accepter.listen_addr;
  my_inst.rank = my_rank;

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
  
  for (map<entity_name_t,entity_inst_t>::iterator it = m->entity_map.begin();
       it != m->entity_map.end();
       it++) {
    dout(10) << "lookup got " << it->first << " at " << it->second << endl;
    entity_name_t addr = it->first;
    entity_inst_t inst = it->second;

    if (down.count(addr)) {
      // ignore
      dout(10) << "ignoring lookup results for " << addr << ", who is down" << endl;
      //assert(entity_map.count(addr) == 0);
      continue;
    }

    if (entity_map.count(addr) &&
        entity_map[addr] > inst) {
      dout(10) << "ignoring lookup results for " << addr << ", " \
               << entity_map[addr] << " > " << inst << endl;
      continue;
    }

    // update map.
    entity_map[addr] = inst;

    if (inst.rank == my_rank) {
      // local
      dout(10) << "delivering lookup results locally" << endl;
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
      // remote
      if (rank_sender.count(inst.rank) == 0) 
        connect_rank(inst);
      else if (rank_sender[inst.rank]->inst != inst) {
        dout(0) << "lookup got rank addr change, WATCH OUT" << endl;
        // FIXME BUG possible message loss weirdness?
        rank_sender[inst.rank]->stop();
        rank_sender.erase(inst.rank);
        connect_rank(inst);
      }
      
      // take waiters
      Sender *sender = rank_sender[inst.rank];
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
      delete messenger;
      lock.Lock();
      continue;
    }

    wait_cond.Wait(lock);
  }
  lock.Unlock();

  // done!  clean up.

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
    for (hash_map<int,Sender*>::iterator i = rank_sender.begin();
         i != rank_sender.end();
         i++)
      i->second->stop();
    while (!rank_sender.empty()) {
      wait_cond.Wait(lock);
      reaper();
    }

    if (0) {  // stop() no worky on receivers!  we leak, but who cares.
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



int Rank::find_ns_addr(tcpaddr_t &nsa)
{
  // file?
  int fd = ::open(".ceph_ns",O_RDONLY);
  if (fd > 0) {
    ::read(fd, (void*)&nsa, sizeof(nsa));
    ::close(fd);
    cout << "ceph ns is " << nsa << endl;
    return 0;
  }

  // env var?
  char *nsaddr = getenv("CEPH_NAMESERVER");////envz_entry(*envp, e_len, "CEPH_NAMESERVER");    
  if (nsaddr) {
    while (nsaddr[0] != '=') nsaddr++;
    nsaddr++;
    
    if (tcp_hostlookup(nsaddr, nsa) < 0) {
      cout << "can't resolve " << nsaddr << endl;
      return -1;
    }

    cout << "ceph ns is " << nsa << endl;
    return 0;
  }

  cerr << "i can't find ceph ns addr in .ceph_ns or CEPH_NAMESERVER" << endl;
  return -1;
}



/**********************************
 * EntityMessenger
 */

Rank::EntityMessenger::EntityMessenger(entity_name_t myaddr) :
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
                  << "---- " 
                  << m->get_source() << ':' << m->get_source_port() 
                  << " to " << m->get_dest() << ':' << m->get_dest_port()
                  << " ---- " << m->get_type_name() 
                  << " ---- " << m->get_source_inst()
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
  if (dispatch_thread.am_self()) {
    dout(1) << "shutdown i am dispatch, setting stop flag" << endl;
    stop = true;
  } else {
    dout(1) << "shutdown i am not dispatch, setting stop flag and joining thread." << endl;
    lock.Lock();
    stop = true;
    cond.Signal();
    lock.Unlock();
    dispatch_thread.join();
  }

  return 0;
}


void Rank::EntityMessenger::prepare_send_message(entity_name_t dest)
{
  rank.prepare_dest(dest);
}

int Rank::EntityMessenger::send_message(Message *m, entity_name_t dest, const entity_inst_t& inst)
{
  // set envelope
  m->set_source(get_myaddr(), 0);
  m->set_dest(dest, 0);

  m->set_source_inst(rank.my_inst);

  dout(1) << "--> " 
          << m->get_source() //<< ':' << m->get_source_port() 
          << " to " << m->get_dest() //<< ':' << m->get_dest_port()
          << " ---- " << m->get_type_name() 
          << " ---- " << rank.my_inst << " --> " << inst
          << " ---- " << m 
          << endl;

  rank.submit_message(m, inst);

  return 0;
}


int Rank::EntityMessenger::send_message(Message *m, entity_name_t dest, int port, int fromport)
{
  // set envelope
  m->set_source(get_myaddr(), fromport);
  m->set_dest(dest, port);

  m->set_source_inst(rank.my_inst);

  dout(1) << "--> " 
          << m->get_source() //<< ':' << m->get_source_port() 
          << " to " << m->get_dest() //<< ':' << m->get_dest_port()
          << " ---- " << m->get_type_name() 
          << " ---- " << rank.my_inst << " --> ?"
          << " ---- " << m 
          << endl;

  rank.submit_message(m);

  return 0;
}


void Rank::EntityMessenger::mark_down(entity_name_t a, entity_inst_t& i)
{
  assert(a != get_myaddr());
  rank.mark_down(a,i);
}

void Rank::mark_down(entity_name_t a, entity_inst_t& inst)
{
  if (my_rank == 0) return;   // ugh.. rank0 already handles this stuff in the namer
  lock.Lock();
  if (down.count(a) == 0) {
    if (entity_map.count(a) &&
        entity_map[a] > inst) {
      dout(10) << "mark_down " << a << " inst " << inst << " < " << entity_map[a] << endl;
      derr(10) << "mark_down " << a << " inst " << inst << " < " << entity_map[a] << endl;
      // do nothing!
    } else {
      down.insert(a);

      if (entity_map.count(a) == 0) {
        // don't know it
        dout(10) << "mark_down " << a << " inst " << inst << " ... unknown by me" << endl;
        derr(10) << "mark_down " << a << " inst " << inst << " ... unknown by me" << endl;

        waiting_for_lookup.erase(a);
        looking_up.erase(a);
      } else {
        // know it
        assert(entity_map[a] <= inst);
        dout(10) << "mark_down " << a << " inst " << inst << endl;
        derr(10) << "mark_down " << a << " inst " << inst << endl;
        
        entity_map.erase(a);
        
        if (rank_sender.count(inst.rank)) {
          rank_sender[inst.rank]->stop();
          rank_sender.erase(inst.rank);
        }
      }
    }
  }
  lock.Unlock();
}

void Rank::EntityMessenger::mark_up(entity_name_t a, entity_inst_t& i)
{
  assert(a != get_myaddr());
  rank.mark_up(a, i);
}

void Rank::mark_up(entity_name_t a, entity_inst_t& i)
{
  if (my_rank == 0) return;
  lock.Lock();
  {
    dout(10) << "mark_up " << a << " inst " << i << endl;
    derr(10) << "mark_up " << a << " inst " << i << endl;

    down.erase(a);

    assert(i.rank != my_rank);     // hrm?
    
    if (entity_map.count(a) == 0 ||
        entity_map[a] < i) {
      entity_map[a] = i;
      connect_rank(i);
    } else if (entity_map[a] == i) {
      dout(10) << "mark_up " << a << " inst " << i << " ... knew it" << endl;
      derr(10) << "mark_up " << a << " inst " << i << " ... knew it" << endl;
    } else {
      dout(-10) << "mark_up " << a << " inst " << i << " < " << entity_map[a] << endl;
      derr(-10) << "mark_up " << a << " inst " << i << " < " << entity_map[a] << endl;
    }

    //if (waiting_for_lookup.count(a))
    //lookup(a);
  }
  lock.Unlock();
}

