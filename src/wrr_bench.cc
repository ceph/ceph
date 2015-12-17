// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "common/PrioritizedQueue.h"
#include "common/WrrQueue.h"
#include "include/assert.h"
#include <iostream>
#include <chrono>

#include "RunningStat.h"

static bool printstrict = false;
typedef std::map<unsigned long long int, unsigned long long int> PrioStat;
typedef unsigned Klass;
typedef unsigned Prio;
typedef unsigned Kost;
typedef unsigned Strick;
typedef std::tuple<Prio, Klass, Kost, unsigned, Strick> Op;
typedef std::chrono::time_point<std::chrono::system_clock> SWatch;

template <typename T>
class Queue {
  T q;
  RunningStat eqtime, misseddq, eqrtime, missedrdq;
  SWatch start, end;

  const static unsigned max_prios = 5; // (0-4) * 64
  const static unsigned klasses = 7;  // Make prime to help get good coverage

  struct Stats {
    PrioStat opdist, sizedist, totalopdist, totalsizedist;
    unsigned long long totalops, totalcost, totalweightops, totalweightcost;
    RunningStat dqtime;

    Stats() :
      totalops(0),
      totalcost(0),
      totalweightops(0),
      totalweightcost(0),
      dqtime()
    {}

    void resetStats() {
      for (PrioStat::iterator i = opdist.begin(); i != opdist.end(); ++i) {
	totalopdist[i->first] -= i->second;
	totalweightops -= (i->first + 1) * i->second;
      }
      for (PrioStat::iterator i = sizedist.begin(); i != sizedist.end(); ++i) {
	totalsizedist[i->first] -= i->second;
	totalweightcost -= (i->first + 1) * i->second;
      }
      opdist.clear();
      sizedist.clear();
      //totalopdist.clear();
      //totalsizedist.clear();
      totalops = 0;
      totalcost = 0;
      //totalweightops = 0;
      //totalweightcost = 0;
      dqtime.Clear();
    }
  };

  // stats for each test and long running stats
  Stats sqstat, nqstat, sqrstat, nqrstat;

  public:
    static Op gen_op(unsigned i, bool randomize = false) {
      // Choose priority, class, cost and 'op' for this op.
      unsigned p, k, c, o, s;
      if (randomize) {
        p = (rand() % max_prios) * 64;
        k = rand() % klasses;
        c = rand() % (1<<22);  // 4M cost
        // Make some of the costs 0, but make sure small costs
        // still work ok.
        if (c > (1<<19) && c < (1<<20)) {
          c = 0;
        }
        s = rand() % 10;
      } else {
        p = (i % max_prios) * 64;
        k = i % klasses;
        c = (i % 8 == 0 || i % 16 == 0) ? 0 : 1 << (i % 23);
	s = i % 7; // Use prime numbers to
      }
      o = rand() % (1<<16);
      return Op(p, k, c, o, s);
    }
    void enqueue_op(Op &op, bool front = CEPH_OP_QUEUE_BACK,
       	unsigned strict = CEPH_OP_CLASS_NORMAL) {
      start = std::chrono::system_clock::now();
      q.enqueue(std::get<1>(op), op, std::get<0>(op),
	            std::get<2>(op), front, strict);
      end = std::chrono::system_clock::now();
      double ticks = std::chrono::duration_cast
	<std::chrono::nanoseconds>(end - start).count();
      eqtime.Push(ticks);
      eqrtime.Push(ticks);
      switch (std::get<4>(op)) {
      case 6:
	// Strict queue
	if (printstrict) {
	  eq_add_stats(sqstat, op);
	  eq_add_stats(sqrstat, op);
	}
	break;
      default:
	//Normal queue
	eq_add_stats(nqstat, op);
	eq_add_stats(nqrstat, op);
	break;
      }
    }
    void eq_add_stats(Stats &s, Op &r) {
	++s.totalopdist[std::get<0>(r)];
	s.totalsizedist[std::get<0>(r)] += std::get<2>(r);
	s.totalweightops += (std::get<0>(r) + 1);
	s.totalweightcost += (std::get<0>(r) +1) * std::get<2>(r);
    }
    Op dequeue_op() {
      Op r;
      unsigned missed;
      start = std::chrono::system_clock::now();
      r = q.dequeue(missed);
      end = std::chrono::system_clock::now();
      // Keep track of strict and normal queues seperatly
      switch (std::get<4>(r)) {
      case 6:
	// Strict queue
	if (printstrict) {
	  dq_add_stats(sqstat, r, end - start);
	  dq_add_stats(sqrstat, r, end - start);
	}
	break;
      default:
	// Normal queue
	// misseddq only makes sense in the normal queue.
	misseddq.Push(missed);
	missedrdq.Push(missed);
	dq_add_stats(nqstat, r, end - start);
	dq_add_stats(nqrstat, r, end - start);
	break;
      }
      return r;
    }
    void dq_add_stats(Stats &s, Op &r,
       	std::chrono::duration<double> t) {
      s.dqtime.Push(std::chrono::duration_cast<std::chrono::nanoseconds>(t).count());
      ++s.opdist[std::get<0>(r)];
      ++s.totalops;
      s.sizedist[std::get<0>(r)] += std::get<2>(r);
      s.totalcost += std::get<2>(r);
    }

    Queue(unsigned max_per = 0, unsigned min_c =0) :
      q(max_per, min_c),
      eqtime(),
      misseddq(),
      sqstat(),
      nqstat(),
      eqrtime(),
      missedrdq(),
      sqrstat(),
      nqrstat()
	{}

    void gen_enqueue(unsigned i, bool randomize = false) {
      unsigned op_queue, fob;
      Op tmpop = gen_op(i, randomize);
      enqueue(i, tmpop, randomize);
    }

    void enqueue(unsigned i, Op tmpop, bool randomize = false,
	int fob = -1) {
      // Choose how to enqueue this op.
      if (randomize) {
        //op_queue = rand() % 10;
        fob = rand() % 10;
      } else {
	if (fob == -1) {
	  //op_queue = i % 7; // Use prime numbers to
          fob = i % 11;     // get better coverage
	}
      }
      switch (std::get<4>(tmpop)) {
      case 6 :
        // Strict Queue
        if (fob == 4) {
          // Queue to the front.
          enqueue_op(tmpop, CEPH_OP_QUEUE_FRONT,
             	CEPH_OP_CLASS_STRICT);
        } else {
          //Queue to the back.
          enqueue_op(tmpop, CEPH_OP_QUEUE_BACK,
             	CEPH_OP_CLASS_STRICT);
        }
        break;
      default:
        // Normal queue
        if (fob == 4) {
          // Queue to the front.
          enqueue_op(tmpop, CEPH_OP_QUEUE_FRONT);
        } else {
          //Queue to the back.
          enqueue_op(tmpop);
        }
        break;
      }
    }
    void dequeue() {
      if (!q.empty()) {
        Op op = dequeue_op();
      }
    }

    void test_queue(unsigned item_size,
       	unsigned eratio, bool randomize = false) {
      for (unsigned i = 1; i <= item_size; ++i) {
	if (rand() % 100 + 1 < eratio) {
	  gen_enqueue(i, randomize);
	} else {
	  if (!q.empty()) {
	    dequeue();
	  } else {
	    gen_enqueue(i, randomize);
	  }
	}
      }
    }

    void print_substat_summary(Stats s, string n) {
      std::cout << ">" << n << " " << std::setw(6) << q.length() <<
        "/" << std::setw(7) << s.totalops <<
        " (" << std::setw(14) << s.totalcost << ") " <<
        ": " << std::setw(7) << eqtime.Mean() <<
        "," << std::setw(7) << eqtime.StandardDeviation() <<
        "," << std::setw(7) << s.dqtime.Mean() <<
        "," << std::setw(7) << s.dqtime.StandardDeviation();
      if (n.compare("S") != 0) {
	std::cout << "," << std::setw(7) << misseddq.Mean() <<
	  "," << std::setw(7) << misseddq.StandardDeviation();
      }
      std::cout << std::endl;
    }

    void print_substat_io(Stats s) {
      unsigned totprio = 0;
      for (PrioStat::iterator i = s.totalopdist.begin(); i != s.totalopdist.end(); ++i) {
	totprio += 1 + i->first;
      }
      double shares = (double) s.totalops / totprio;
      for (PrioStat::reverse_iterator i = s.totalopdist.rbegin(); i != s.totalopdist.rend(); ++i) {
	unsigned long long dqops, prio;
	prio = i->first;
	PrioStat::iterator it = s.opdist.find(prio);
	if (it != s.opdist.end()) {
	  dqops = it->second;
	} else {
	  dqops = 0;
	}
	double availops = (prio + 1) * shares;
	unsigned long long int ops = s.totalopdist[prio];
	//std::cout << totprio << "," << shares << "," << availops <<
	//  "," << ops << std::endl;
	totprio -= (prio + 1);
	if (availops > ops) {
	  shares += (availops - ops) / totprio;
	  availops = ops;
	}
	//std::cout << availops << " / " << s.totalops << std::endl; 
        std::cout << ">>" << std::setw(3) << prio <<
          ":" << std::setw(15) << dqops <<
          "/" << std::setw(15) << s.totalopdist[prio] <<
          " " << std::setw(6) << std::fixed << std::setprecision(2) <<
          ((double)dqops / s.totalopdist[prio]) * 100 << " % " <<
          " (" << std::setw(6) << std::fixed << std::setprecision(2);
	if (s.totalops != 0) {
	  std::cout << ((double)dqops / s.totalops) * 100 << " %/" <<
	    std::setw(6) << std::fixed << std::setprecision(2) <<
	    (availops / s.totalops) * 100 << " %/";
	} else {
	  std::cout << "0.00" << " %/" <<
	    std::setw(6) << std::fixed << std::setprecision(2) <<
	    "0.00" << " %/";
	}
	  std::cout << std::setw(6) << std::fixed << std::setprecision(2) <<
          ((double) ((unsigned long long int)(prio + 1) * s.totalopdist[prio]) / s.totalweightops) * 100 << " %)" <<
          std::endl;
      }
    }

    void print_substat_cost(Stats s) {
      unsigned totprio = 0;
      for (PrioStat::iterator i = s.totalopdist.begin(); i != s.totalopdist.end(); ++i) {
	totprio += 1 + i->first;
      }
      double shares = (double) s.totalcost / totprio;
      for (PrioStat::reverse_iterator i = s.totalsizedist.rbegin(); i != s.totalsizedist.rend(); ++i) {
	unsigned long long dqcost, prio;
	prio = i->first;
	PrioStat::iterator it = s.sizedist.find(prio);
	if (it != s.sizedist.end()) {
	  dqcost = it->second;
	} else {
	  dqcost = 0;
	}
	double availcost = (prio + 1) * shares;
	unsigned long long int cost = s.totalsizedist[prio];
	totprio -= (prio + 1);
	if (availcost > cost) {
	  shares += (availcost - cost) / totprio;
	  availcost = cost;
	}
        std::cout << ">>" << std::setw(3) << prio <<
          ":" << std::setw(15) << dqcost <<
          "/" << std::setw(15) << s.totalsizedist[prio] <<
          " " << std::setw(6) << std::fixed << std::setprecision(2) <<
          ((double)dqcost / s.totalsizedist[prio]) * 100 << " % " <<
          " (" << std::setw(6) << std::fixed << std::setprecision(2);
	if (s.totalcost != 0) {
	  std::cout << ((double) i->second / s.totalcost) * 100 << " %/" <<
	    std::setw(6) << std::fixed << std::setprecision(2) <<
	    (availcost / s.totalcost) * 100 << " %/";
	} else {
	  std::cout << "0.00" << " %/" <<
	    std::setw(6) << std::fixed << std::setprecision(2) <<
	  "0.00" << " %/";
	}
	std::cout << std::setw(6) << std::fixed << std::setprecision(2) <<
          ((double) ((unsigned long long int)(i->first + 1) * i->second) / s.totalweightcost) * 100 << " %)" <<
          std::endl;
      }
    }

    void print_stats() {
      std::cout << ">" << "Q " << std::setw(6) << "len" <<
        "/" << std::setw(7) << "DQ ops" <<
        " (" << std::setw(14) << "T. cost" << ") " <<
        ": " << std::setw(7) << "E. Mn" <<
        "," << std::setw(7) << "E. SD" <<
        "," << std::setw(7) << "D. Mn" <<
        "," << std::setw(7) << "D. SD" <<
        "," << std::setw(7) << "M. Mn" <<
        "," << std::setw(7) << "M. SD" << std::endl;
      if (printstrict) {
	print_substat_summary(sqstat, "S");
	print_substat_summary(sqrstat, "R");
      }
      print_substat_summary(nqstat, "N");
      print_substat_summary(nqrstat, "R");

      std::cout << ">>" << "  P:" <<
       	std::setw(15) << "DQ OPs/Cost" <<
        "/" << std::setw(15) << "T. OPs/Cost" <<
        " " << std::setw(6) << "DQ" << " % " <<
        " (" << std::setw(6) << "A Dist" << " %/" <<
	std::setw(6) << "C Dist" << " %/" <<
        std::setw(6) << "P Dist" << " %)" <<
	std::endl;
      if (printstrict) {
	print_substat_io(sqstat);
	print_substat_io(sqrstat);
	print_substat_cost(sqstat);
	print_substat_cost(sqrstat);
      }
      print_substat_io(nqstat);
      std::cout << ">>" << std::setfill('-') << std::setw(74) <<
	"-" << std::setfill(' ') << std::endl;
      print_substat_io(nqrstat);
      std::cout << ">>" << std::setfill('=') << std::setw(74) <<
	"=" << std::setfill(' ') << std::endl;
      print_substat_cost(nqstat);
      std::cout << ">>" << std::setfill('-') << std::setw(74) <<
	"-" << std::setfill(' ') << std::endl;
      print_substat_cost(nqrstat);
    }
    void reset_round_stats() {
      eqtime.Clear();
      misseddq.Clear();
      sqstat.resetStats();
      nqstat.resetStats();
    }
    bool empty() const {
      return q.empty();
    }
};

typedef Queue<PrioritizedQueue<Op, unsigned>> PQ;
typedef Queue<WrrQueue<Op, unsigned>> WQ;

void work_queues(PQ &pq, WQ &wq, int count, int eratio, string name) {
  wq.reset_round_stats();
  pq.reset_round_stats();
  unsigned long long enqueue = 0, dequeue = 0;
  for (unsigned i = 0; i < count; ++i) {
    if (eratio <= 10 && pq.empty()) {
      std::cout << "Nothing left to do, breaking early." << std::endl;
      break;
    }
    if (rand() % 101 < eratio) {
      ++enqueue;
      unsigned op_queue, fob;
      Op tmpop = Queue<int>::gen_op(i, true);
      // Choose how to enqueue this op.
      op_queue = rand() % 10;
      fob = rand() % 10;
      pq.enqueue(i, tmpop, false, fob);
      wq.enqueue(i, tmpop, false, fob);
    } else {
      ++dequeue;
      pq.dequeue();
      wq.dequeue();
    }
  }
  std::cout << std::endl;
  std::cout << enqueue << "/" << dequeue << " (" <<std::setw(6) <<
    std::fixed << std::setprecision(2) << (double) enqueue / (enqueue + dequeue) * 100 <<
    " % / " << std::setw(6) << std::fixed << std::setprecision(2) <<
    (double) dequeue / (enqueue + dequeue) * 100 << " %)" << std::endl;
  std::cout << "===Prio Queue stats (" << name << "):" << std::endl;
  pq.print_stats();
  std::cout << "===Wrr Queue stats (" << name << "):" << std::endl;
  wq.print_stats();
}

int main() {
  srand(time(0));
  PQ pq;
  WQ wq;
  std::cout << "Running.." << std::endl;
  work_queues(pq, wq, 10000, 100, "Warm-up (100/0)");
  work_queues(pq, wq, 100000, 70, "Stress (70/30)");
  work_queues(pq, wq, 1000000, 50, "Balanced (50/50)");
  work_queues(pq, wq, 100000, 30, "Cool-down (30/70)");
  work_queues(pq, wq, 1000000, 0, "Drain (0/100)");


}
