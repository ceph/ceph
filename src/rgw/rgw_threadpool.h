#ifndef _RGW_THREADPOOL_
#define _RGW_THREADPOOL_

#include <queue>

class WorkerThread {
public:
  WorkerThread() 
  {
    state = EState_None;
    handle = 0;
  }

  virtual ~WorkerThread()
  {
    assert(state != EState_Started || joined);
  }

  void start()
  {
    assert(state == EState_None);
    if (pthread_create(&handle, NULL, threadProc, this))
      abort();
    state = EState_Started;
  }

  void join()
  {
    assert(state == EState_Started);
    pthread_join(handle, NULL);
    state = EState_Joined;
  }

protected:
  virtual void run() = 0;

private:
  static void* threadProc(void* param)
  {
    WorkerThread* thread = reinterpret_cast<WorkerThread*>(param);
    thread->run();
    return NULL;
  }

private:
  enum EState
  {
    EState_None,
    EState_Started,
    EState_Joined
  };

  EState state;
  bool joined;
  pthread_t handle;
protected:
  CURL *curl;
};

class Task {
public:
  Task() {}
  virtual ~Task() {}
  virtual void run()=0;
  virtual void set_handler(void*) = 0;
};


class WorkQueue {
public:
  WorkQueue() {
    pthread_mutex_init(&qmtx,0);
    pthread_cond_init(&wcond, 0);
  }

  ~WorkQueue() {
    pthread_mutex_destroy(&qmtx);
    pthread_cond_destroy(&wcond);
  }

  Task *nextTask() {
    Task *nt = 0;
    pthread_mutex_lock(&qmtx);
    while (tasks.empty()){
      pthread_cond_wait(&wcond, &qmtx);
    }
    nt = tasks.front();
    tasks.pop();
    pthread_mutex_unlock(&qmtx);
    return nt;
  }

  void addTask(Task *nt) {
    pthread_mutex_lock(&qmtx);
    tasks.push(nt);
    pthread_cond_signal(&wcond);
    pthread_mutex_unlock(&qmtx);
  }

private:
  std::queue<Task*> tasks;
  pthread_mutex_t qmtx;
  pthread_cond_t wcond;
};

class PoolWorkerThread : public WorkerThread{
public:
  PoolWorkerThread(WorkQueue& _work_queue) : work_queue(_work_queue) {}
protected:
  virtual void run() {
    while (Task* task = work_queue.nextTask()) {
      task->set_handler((void *)curl);
      task->run();
    }
  }
private:
  WorkQueue& work_queue;
};

#endif

