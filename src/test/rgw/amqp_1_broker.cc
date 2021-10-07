// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef OPTIONS_HPP
#define OPTIONS_HPP
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <string>
#include <sstream>
#include <ostream>
#include <vector>
#include <stdexcept>

namespace example {
  /** bad_option is thrown for option parsing errors */
  struct bad_option : public std::runtime_error {
    bad_option(const std::string& s) : std::runtime_error(s) {}
  };

  /** Simple command-line option parser for example programs */
  class options {
    public:

      options(int argc, char const * const * argv) : argc_(argc), argv_(argv), prog_(argv[0]), help_() {
        size_t slash = prog_.find_last_of("/\\");
        if (slash != std::string::npos)
          prog_ = prog_.substr(slash+1); // Extract prog name from path
        add_flag(help_, 'h', "help", "Print the help message");
      }

      ~options() {
        for (opts::iterator i = opts_.begin(); i != opts_.end(); ++i)
          delete *i;
      }

      /** Updates value when parse() is called if option is present with a value. */
      template<class T>
        void add_value(T& value, char short_name, const std::string& long_name, const std::string& description, const std::string var) {
          opts_.push_back(new option_value<T>(value, short_name, long_name, description, var));
        }

      /** Sets flag when parse() is called if option is present. */
      void add_flag(bool& flag, char short_name, const std::string& long_name, const std::string& description) {
        opts_.push_back(new option_flag(flag, short_name, long_name, description));
      }

      /** Parse the command line, return the index of the first non-option argument.
       *@throws bad_option if there is a parsing error or unknown option.
       */
      int parse() {
        int arg = 1;
        for (; arg < argc_ && argv_[arg][0] == '-'; ++arg) {
          opts::iterator i = opts_.begin();
          while (i != opts_.end() && !(*i)->parse(argc_, argv_, arg))
            ++i;
          if (i == opts_.end())
            throw bad_option(std::string("unknown option ") + argv_[arg]);
        }
        if (help_) throw bad_option("");
        return arg;
      }

      /** Print a usage message */
      friend std::ostream& operator<<(std::ostream& os, const options& op) {
        os << std::endl << "usage: " << op.prog_ << " [options]" << std::endl;
        os << std::endl << "options:" << std::endl;
        for (opts::const_iterator i = op.opts_.begin(); i < op.opts_.end(); ++i)
          os << **i << std::endl;
        return os;
      }

    private:
      class option {
        public:
          option(char s, const std::string& l, const std::string& d, const std::string v) :
            short_(std::string("-") + s), long_("--" + l), desc_(d), var_(v) {}
          virtual ~option() {}

          virtual bool parse(int argc, char const * const * argv, int &i) = 0;
          virtual void print_default(std::ostream&) const {}

          friend std::ostream& operator<<(std::ostream& os, const option& op) {
            os << "  " << op.short_;
            if (!op.var_.empty()) os << " " << op.var_;
            os << ", " << op.long_;
            if (!op.var_.empty()) os << "=" << op.var_;
            os << std::endl << "        " << op.desc_;
            op.print_default(os);
            return os;
          }

        protected:
          std::string short_, long_, desc_, var_;
      };

      template <class T>
        class option_value : public option {
          public:
            option_value(T& value, char s, const std::string& l, const std::string& d, const std::string& v) :
              option(s, l, d, v), value_(value) {}

            bool parse(int argc, char const * const * argv, int &i) {
              std::string arg(argv[i]);
              if (arg == short_ || arg == long_) {
                if (i < argc-1) {
                  set_value(arg, argv[++i]);
                  return true;
                } else {
                  throw bad_option("missing value for " + arg);
                }
              }
              if (arg.compare(0, long_.size(), long_) == 0 && arg[long_.size()] == '=' ) {
                set_value(long_, arg.substr(long_.size()+1));
                return true;
              }
              return false;
            }

            virtual void print_default(std::ostream& os) const { os << " (default " << value_ << ")"; }

            void set_value(const std::string& opt, const std::string& s) {
              std::istringstream is(s);
              is >> value_;
              if (is.fail() || is.bad())
                throw bad_option("bad value for " + opt + ": " + s);
            }

          private:
            T& value_;
        };

      class option_flag: public option {
        public:
          option_flag(bool& flag, const char s, const std::string& l, const std::string& d) :
            option(s, l, d, ""), flag_(flag)
        { flag_ = false; }

          bool parse(int /*argc*/, char const * const * argv, int &i) {
            if (argv[i] == short_ || argv[i] == long_) {
              flag_ = true;
              return true;
            } else {
              return false;
            }
          }

        private:
          bool &flag_;
      };

      typedef std::vector<option*> opts;

      int argc_;
      char const * const * argv_;
      std::string prog_;
      opts opts_;
      bool help_;
  };
}

#endif // OPTIONS_HPP


#include <proton/connection.hpp>
#include <proton/connection_options.hpp>
#include <proton/container.hpp>
#include <proton/delivery.hpp>
#include <proton/error_condition.hpp>
#include <proton/listen_handler.hpp>
#include <proton/listener.hpp>
#include <proton/message.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/receiver_options.hpp>
#include <proton/sender_options.hpp>
#include <proton/source_options.hpp>
#include <proton/target.hpp>
#include <proton/target_options.hpp>
#include <proton/tracker.hpp>
#include <proton/transport.hpp>
#include <proton/work_queue.hpp>

#include <deque>
#include <iostream>
#include <map>
#include <string>

#if PN_CPP_HAS_STD_THREAD
#include <thread>
#endif

// legacy from qpid-proton-cpp
#define OVERRIDE override

// This is a simplified model for a message broker, that only allows for
// messages to go to a single receiver.
//
// This broker is multithread safe and if compiled with C++11 with a multithreaded Proton
// binding library will use as many threads as there are thread resources available (usually
// cores)
//
// Queues are only created and never destroyed
//
// Broker Entities (that need to be individually serialised)
// QueueManager - Creates new queues, finds queues
// Queue        - Queues msgs, records subscribers, sends msgs to subscribers
// Connection   - Receives Messages from network, sends messages to network.

// Work
// FindQueue(queueName, connection) - From a Connection to the QueueManager
//     This will create the queue if it doesn't already exist and send a BoundQueue
//     message back to the connection.
// BoundQueue(queue) - From the QueueManager to a Connection
//
// QueueMsg(msg)        - From a Connection (receiver) to a Queue
// Subscribe(sender)    - From a Connection (sender) to a Queue
// Flow(sender, credit) - From a Connection (sender) to a Queue
// Unsubscribe(sender)  - From a Connection (sender) to a Queue
//
// SendMsg(msg)   - From a Queue to a Connection (sender)
// Unsubscribed() - From a Queue to a Connection (sender)


// Simple debug output
bool verbose;
#define DOUT(x) do {if (verbose) {x};} while (false)

class Queue;
class Sender;

typedef std::map<proton::sender, Sender*> senders;

class Sender : public proton::messaging_handler {
  friend class connection_handler;

  proton::sender sender_;
  senders& senders_;
  proton::work_queue& work_queue_;
  std::string queue_name_;
  Queue* queue_;
  int pending_credit_;

  // Messaging handlers
  void on_sendable(proton::sender &sender) OVERRIDE;
  void on_sender_close(proton::sender &sender) OVERRIDE;

  public:
  Sender(proton::sender s, senders& ss) :
    sender_(s), senders_(ss), work_queue_(s.work_queue()), queue_(0), pending_credit_(0)
  {}

  bool add(proton::work f) {
    return work_queue_.add(f);
  }


  void boundQueue(Queue* q, std::string qn);
  void sendMsg(proton::message m) {
    DOUT(std::cerr << "Sender:   " << this << " sending\n";);
    sender_.send(m);
  }
  void unsubscribed() {
    DOUT(std::cerr << "Sender:   " << this << " deleting\n";);
    delete this;
  }
};

// Queue - round robin subscriptions
class Queue {
  proton::work_queue work_queue_;
  const std::string name_;
  std::deque<proton::message> messages_;
  typedef std::map<Sender*, int> subscriptions; // With credit
  subscriptions subscriptions_;
  subscriptions::iterator current_;

  void tryToSend() {
    DOUT(std::cerr << "Queue:    " << this << " tryToSend: " << subscriptions_.size(););
    // Starting at current_, send messages to subscriptions with credit:
    // After each send try to find another subscription; Wrap around;
    // Finish when we run out of messages or credit.
    size_t outOfCredit = 0;
    while (!messages_.empty() && outOfCredit<subscriptions_.size()) {
      // If we got the end (or haven't started yet) start at the beginning
      if (current_==subscriptions_.end()) {
        current_=subscriptions_.begin();
      }
      // If we have credit send the message
      DOUT(std::cerr << "(" << current_->second << ") ";);
      if (current_->second>0) {
        DOUT(std::cerr << current_->first << " ";);
        current_->first->add(make_work(&Sender::sendMsg, current_->first, messages_.front()));
        messages_.pop_front();
        --current_->second;
        ++current_;
      } else {
        ++outOfCredit;
      }
    }
    DOUT(std::cerr << "\n";);
  }

  public:
  Queue(proton::container& c, const std::string& n) :
    work_queue_(c), name_(n), current_(subscriptions_.end())
  {}

  bool add(proton::work f) {
    return work_queue_.add(f);
  }

  void queueMsg(proton::message m) {
    DOUT(std::cerr << "Queue:    " << this << "(" << name_ << ") queueMsg\n";);
    messages_.push_back(m);
    tryToSend();
  }
  void flow(Sender* s, int c) {
    DOUT(std::cerr << "Queue:    " << this << "(" << name_ << ") flow: " << c << " to " << s << "\n";);
    subscriptions_[s] = c;
    tryToSend();
  }
  void subscribe(Sender* s) {
    DOUT(std::cerr << "Queue:    " << this << "(" << name_ << ") subscribe Sender: " << s << "\n";);
    subscriptions_[s] = 0;
  }
  void unsubscribe(Sender* s) {
    DOUT(std::cerr << "Queue:    " << this << "(" << name_ << ") unsubscribe Sender: " << s << "\n";);
    // If we're about to erase the current subscription move on
    if (current_ != subscriptions_.end() && current_->first==s) ++current_;
    subscriptions_.erase(s);
    s->add(make_work(&Sender::unsubscribed, s));
  }
};

// We have credit to send a message.
void Sender::on_sendable(proton::sender &sender) {
  if (queue_) {
    queue_->add(make_work(&Queue::flow, queue_, this, sender.credit()));
  } else {
    pending_credit_ = sender.credit();
  }
}

void Sender::on_sender_close(proton::sender &sender) {
  if (queue_) {
    queue_->add(make_work(&Queue::unsubscribe, queue_, this));
  } else {
    // TODO: Is it possible to be closed before we get the queue allocated?
    // If so, we should have a way to mark the sender deleted, so we can delete
    // on queue binding
  }
  senders_.erase(sender);
}

void Sender::boundQueue(Queue* q, std::string qn) {
  DOUT(std::cerr << "Sender:   " << this << " bound to Queue: " << q <<"(" << qn << ")\n";);
  queue_ = q;
  queue_name_ = qn;

  q->add(make_work(&Queue::subscribe, q, this));
  sender_.open(proton::sender_options()
      .source((proton::source_options().address(queue_name_)))
      .handler(*this));
  if (pending_credit_>0) {
    queue_->add(make_work(&Queue::flow, queue_, this, pending_credit_));
  }
  std::cout << "sending from " << queue_name_ << std::endl;
}

class Receiver : public proton::messaging_handler {
  friend class connection_handler;

  proton::receiver receiver_;
  proton::work_queue& work_queue_;
  Queue* queue_;
  std::deque<proton::message> messages_;

  // A message is received.
  void on_message(proton::delivery &, proton::message &m) OVERRIDE {
    messages_.push_back(m);

    if (queue_) {
      queueMsgs();
    }
  }

  void queueMsgs() {
    DOUT(std::cerr << "Receiver: " << this << " queueing " << messages_.size() << " msgs to: " << queue_ << "\n";);
    while (!messages_.empty()) {
      queue_->add(make_work(&Queue::queueMsg, queue_, messages_.front()));
      messages_.pop_front();
    }
  }

  public:
  Receiver(proton::receiver r) :
    receiver_(r), work_queue_(r.work_queue()), queue_(0)
  {}

  bool add(proton::work f) {
    return work_queue_.add(f);
  }

  void boundQueue(Queue* q, std::string qn) {
    DOUT(std::cerr << "Receiver: " << this << " bound to Queue: " << q << "(" << qn << ")\n";);
    queue_ = q;
    receiver_.open(proton::receiver_options()
        .source((proton::source_options().address(qn)))
        .handler(*this));
    std::cout << "receiving to " << qn << std::endl;

    queueMsgs();
  }
};

class QueueManager {
  proton::container& container_;
  proton::work_queue work_queue_;
  typedef std::map<std::string, Queue*> queues;
  queues queues_;
  int next_id_; // Use to generate unique queue IDs.

  public:
  QueueManager(proton::container& c) :
    container_(c), work_queue_(c), next_id_(0)
  {}

  bool add(proton::work f) {
    return work_queue_.add(f);
  }

  template <class T>
    void findQueue(T& connection, std::string& qn) {
      if (qn.empty()) {
        // Dynamic queue creation
        std::ostringstream os;
        os << "_dynamic_" << next_id_++;
        qn = os.str();
      }
      Queue* q = 0;
      queues::iterator i = queues_.find(qn);
      if (i==queues_.end()) {
        q = new Queue(container_, qn);
        queues_[qn] = q;
      } else {
        q = i->second;
      }
      connection.add(make_work(&T::boundQueue, &connection, q, qn));
    }

  void findQueueSender(Sender* s, std::string qn) {
    findQueue(*s, qn);
  }

  void findQueueReceiver(Receiver* r, std::string qn) {
    findQueue(*r, qn);
  }
};

class connection_handler : public proton::messaging_handler {
  QueueManager& queue_manager_;
  senders senders_;

  public:
  connection_handler(QueueManager& qm) :
    queue_manager_(qm)
  {}

  void on_connection_open(proton::connection& c) OVERRIDE {
    c.open();            // Accept the connection
  }

  // A sender sends messages from a queue to a subscriber.
  void on_sender_open(proton::sender &sender) OVERRIDE {
    std::string qn = sender.source().dynamic() ? "" : sender.source().address();
    Sender* s = new Sender(sender, senders_);
    senders_[sender] = s;
    queue_manager_.add(make_work(&QueueManager::findQueueSender, &queue_manager_, s, qn));
  }

  // A receiver receives messages from a publisher to a queue.
  void on_receiver_open(proton::receiver &receiver) OVERRIDE {
    std::string qname = receiver.target().address();
    Receiver* r = new Receiver(receiver);
    queue_manager_.add(make_work(&QueueManager::findQueueReceiver, &queue_manager_, r, qname));
  }

  void on_session_close(proton::session &session) OVERRIDE {
    // Unsubscribe all senders that belong to session.
    for (proton::sender_iterator i = session.senders().begin(); i != session.senders().end(); ++i) {
      senders::iterator j = senders_.find(*i);
      if (j == senders_.end()) continue;
      Sender* s = j->second;
      if (s->queue_) {
        s->queue_->add(make_work(&Queue::unsubscribe, s->queue_, s));
      }
      senders_.erase(j);
    }
  }

  void on_error(const proton::error_condition& e) OVERRIDE {
    std::cout << "protocol error: " << e.what() << std::endl;
  }

  // The container calls on_transport_close() last.
  void on_transport_close(proton::transport& t) OVERRIDE {
    // Unsubscribe all senders.
    for (proton::sender_iterator i = t.connection().senders().begin(); i != t.connection().senders().end(); ++i) {
      senders::iterator j = senders_.find(*i);
      if (j == senders_.end()) continue;
      Sender* s = j->second;
      if (s->queue_) {
        s->queue_->add(make_work(&Queue::unsubscribe, s->queue_, s));
      }
    }
    delete this;            // All done.
  }
};

class broker {
  public:
    broker(const std::string addr) :
      container_("broker"), queues_(container_), listener_(queues_)
  {
    container_.listen(addr, listener_);
  }

    void run() {
#if PN_CPP_HAS_STD_THREAD
      container_.run(std::thread::hardware_concurrency());
#else
      container_.run();
#endif
    }

  private:
    struct listener : public proton::listen_handler {
      listener(QueueManager& c) : queues_(c) {}

      proton::connection_options on_accept(proton::listener&) OVERRIDE{
        return proton::connection_options().handler(*(new connection_handler(queues_)));
      }

      void on_open(proton::listener& l) OVERRIDE {
        std::cout << "broker listening on " << l.port() << std::endl;
      }

      void on_error(proton::listener&, const std::string& s) OVERRIDE {
        std::cerr << "listen error: " << s << std::endl;
        throw std::runtime_error(s);
      }
      QueueManager& queues_;
    };

    proton::container container_;
    QueueManager queues_;
    listener listener_;
};

int main(int argc, char **argv) {
  // Command line options
  std::string address("0.0.0.0");
  example::options opts(argc, argv);

  opts.add_flag(verbose, 'v', "verbose", "verbose (debugging) output");
  opts.add_value(address, 'a', "address", "listen on URL", "URL");

  try {
    verbose = false;
    opts.parse();
    broker(address).run();
    return 0;
  } catch (const example::bad_option& e) {
    std::cout << opts << std::endl << e.what() << std::endl;
  } catch (const std::exception& e) {
    std::cout << "broker shutdown: " << e.what() << std::endl;
  }
  return 1;
}
