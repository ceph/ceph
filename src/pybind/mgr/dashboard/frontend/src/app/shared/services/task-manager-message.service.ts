import { Injectable } from '@angular/core';
import { FinishedTask } from '../models/finished-task';
import { Task } from '../models/task';

class TaskManagerMessage {
  descr: (metadata) => string;
  success: (metadata) => string;
  error: (metadata) => object;

  constructor(descr: (metadata) => string,
              success: (metadata) => string,
              error: (metadata) => object) {
    this.descr = descr;
    this.success = success;
    this.error = error;
  }
}

@Injectable()
export class TaskManagerMessageService {

  messages = {
  };

  defaultMessage = new TaskManagerMessage(
    (metadata) => 'Unknown Task',
    (metadata) => 'Task executed successfully',
    () => {
      return {
      };
    }
  );

  constructor() { }

  getSuccessMessage(finishedTask: FinishedTask) {
    const taskManagerMessage = this.messages[finishedTask.name] || this.defaultMessage;
    return taskManagerMessage.success(finishedTask.metadata);
  }

  getErrorMessage(finishedTask: FinishedTask) {
    const taskManagerMessage = this.messages[finishedTask.name] || this.defaultMessage;
    return taskManagerMessage.error(finishedTask.metadata)[finishedTask.exception.errno] ||
      finishedTask.exception.detail;
  }

  getDescription(task: Task) {
    const taskManagerMessage = this.messages[task.name] || this.defaultMessage;
    return taskManagerMessage.descr(task.metadata);
  }
}
