// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/Context.h"
#include <set>

/**
 * The Continuation interface is designed to help easily create multi-step
 * operations that share data without having to pass it around or create
 * custom Context classes for each step. To write a Continuation:
 * 1) create a child class with a function for each stage.
 * 2) Put all your shared data members into the class.
 * 3) In the constructor, register each function stage with set_callback().
 * 4) Whenever you need to provide a Context callback that activates the next
 * stage, call get_callback(stage_number). If you need to proceed to another
 * stage immediately, call immediate(stage, retcode) and return its result.
 *
 * To use a class:
 * 1) Construct the child class on the heap.
 * 2) Call begin().
 * 3) The destructor will be called once one of your functions returns true to
 * indicate it is done.
 *
 * Please note that while you can skip stages and get multiple Callback
 * objects at once, you *cannot* have any stage report that the Continuation
 * is completed while any other stage Callbacks are outstanding. It's best to
 * be serial unless you want to maintain your own metadata about which stages
 * are still pending.
 *
 * In fact, there are only two situations in which a stage should return
 * true while others are running:
 * 1) A Callback was issued and completed in the same thread,
 * 2) you called immediate(stage) and it is returning true.
 */

class Continuation {
  std::set<int> stages_in_flight;
  std::set<int> stages_processing;
  int rval;
  Context *on_finish;
  bool reported_done;

  class Callback : public Context {
    Continuation *continuation;
    int stage_to_activate;
  public:
    Callback(Continuation *c, int stage) :
      continuation(c),
      stage_to_activate(stage) {}
    void finish(int r) {
      continuation->continue_function(r, stage_to_activate);
    }
  };

protected:
  typedef bool (Continuation::*stagePtr)(int r);
  /**
   * Continue immediately to the given stage. It will be executed
   * immediately, in the given thread.
   * @pre You are in a callback function.
   * @param stage The stage to execute
   * @param r The return code that will be provided to the next stage
   */
  bool immediate(int stage, int r) {
    assert(!stages_in_flight.count(stage));
    assert(!stages_processing.count(stage));
    stages_in_flight.insert(stage);
    stages_processing.insert(stage);
    return _continue_function(r, stage);
  }

  /**
   * Obtain a Context * that when complete()ed calls back into the given stage.
   * @pre You are in a callback function.
   * @param stage The stage this Context should activate
   */
  Context *get_callback(int stage) {
    stages_in_flight.insert(stage);
    return new Callback(this, stage);
  }

  /**
   * Set the return code that is passed to the finally-activated Context.
   * @param new_rval The return code to use.
   */
  void set_rval(int new_rval) { rval = new_rval; }
  int get_rval() { return rval; }

  /**
   * Register member functions as associated with a given stage. Start
   * your stage IDs at 0 and make that one the setup phase.
   * @pre There are no other functions associated with the stage.
   * @param stage The stage to associate this function with
   * @param func The function to use
   */
  void set_callback(int stage, stagePtr func) {
    assert(callbacks.find(stage) == callbacks.end());
    callbacks[stage] = func;
  }
  
  /**
   * Called when the Continuation is done, as determined by a stage returning
   * true and us having finished all the currently-processing ones.
   */
   virtual void _done() {
     on_finish->complete(rval);
     on_finish = NULL;
     return;
   }

private:
  std::map<int, Continuation::stagePtr> callbacks;

  bool _continue_function(int r, int n) {
    set<int>::iterator stage_iter = stages_in_flight.find(n);
    assert(stage_iter != stages_in_flight.end());
    assert(callbacks.count(n));
    stagePtr p = callbacks[n];

    pair<set<int>::iterator,bool> insert_r = stages_processing.insert(n);

    bool done = (this->*p)(r);
    if (done)
      reported_done = true;

    stages_processing.erase(insert_r.first);
    stages_in_flight.erase(stage_iter);
    return done;
  }

  void continue_function(int r, int stage) {
    bool done = _continue_function(r, stage);

    assert (!done ||
            stages_in_flight.size() == stages_processing.size());

    if (done ||
        (reported_done && stages_processing.empty())) {
      _done();
      delete this;
    }
  }



public:
  /**
   * Construct a new Continuation object. Call this from your child class,
   * obviously.
   *
   * @Param c The Context which should be complete()ed when this Continuation
   * is done.
   */
  Continuation(Context *c) :
    rval(0), on_finish(c), reported_done(false) {}
  /**
   * Clean up.
   */
  virtual ~Continuation() { assert(on_finish == NULL); }
  /**
   * Begin running the Continuation.
   */
  void begin() { stages_in_flight.insert(0); continue_function(0, 0); }
};
