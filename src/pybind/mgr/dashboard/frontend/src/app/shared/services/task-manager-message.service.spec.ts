import * as _ from 'lodash';

import { FinishedTask } from '../models/finished-task';
import { TaskException } from '../models/task-exception';
import { TaskManagerMessageService } from './task-manager-message.service';

describe('TaskManagerMessageService', () => {
  let service: TaskManagerMessageService;
  let finishedTask: FinishedTask;

  beforeEach(() => {
    service = new TaskManagerMessageService();
    finishedTask = new FinishedTask();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should getDescription', () => {
    finishedTask.name = 'foo';
    finishedTask.exception = _.assign(new TaskException(), {
      code: 1
    });
    finishedTask.metadata = {};

    const message = service.getDescription(finishedTask);
    expect(message).toBe('Unknown Task');
  });

  it('should get default running message', () => {
    finishedTask.metadata = {};
    let message = service.getRunningMessage(finishedTask);
    expect(message).toBe('Executing unknown task');
    finishedTask.metadata = { component: 'rbd' };
    message = service.getRunningMessage(finishedTask);
    expect(message).toBe('Executing RBD');
  });

  it('should get custom running message', () => {
    finishedTask.name = 'rbd/create';
    finishedTask.metadata = {
      pool_name: 'somePool',
      image_name: 'someImage'
    };
    const message = service.getRunningMessage(finishedTask);
    expect(message).toBe(`Creating RBD 'somePool/someImage'`);
  });

  it('should getErrorMessage', () => {
    finishedTask.exception = _.assign(new TaskException(), {
      code: 1
    });
    const message = service.getErrorMessage(finishedTask);
    expect(message).toBe(undefined);
  });

  it('should getSuccessMessage', () => {
    const message = service.getSuccessMessage(finishedTask);
    expect(message).toBe('Task executed successfully');
  });

  it('should test if all messages methods are defined', () => {
    _.forIn(service.messages, (value, key) => {
      expect(value.descr({})).toBeTruthy();
      expect(value.success({})).toBeTruthy();
      expect(value.error({})).toBeTruthy();
      expect(value.running({})).toBeTruthy();
    });
  });
});
