import { HttpClientTestingModule } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { of } from 'rxjs';

import {
  configureTestBed,
  expectItemTasks,
  i18nProviders
} from '../../../testing/unit-test-helper';
import { RbdService } from '../api/rbd.service';
import { ExecutingTask } from '../models/executing-task';
import { SummaryService } from './summary.service';
import { TaskListService } from './task-list.service';
import { TaskMessageService } from './task-message.service';

describe('TaskListService', () => {
  let service: TaskListService;
  let summaryService: SummaryService;
  let taskMessageService: TaskMessageService;

  let list: any[];
  let apiResp: any;
  let tasks: any[];

  const addItem = (name: string) => {
    apiResp.push({ name: name });
  };

  configureTestBed({
    providers: [TaskListService, TaskMessageService, SummaryService, i18nProviders, RbdService],
    imports: [HttpClientTestingModule, RouterTestingModule]
  });

  beforeEach(() => {
    service = TestBed.inject(TaskListService);
    summaryService = TestBed.inject(SummaryService);
    taskMessageService = TestBed.inject(TaskMessageService);
    summaryService['summaryDataSource'].next({ executing_tasks: [] });

    taskMessageService.messages['test/create'] = taskMessageService.messages['rbd/create'];
    taskMessageService.messages['test/edit'] = taskMessageService.messages['rbd/edit'];
    taskMessageService.messages['test/delete'] = taskMessageService.messages['rbd/delete'];

    tasks = [];
    apiResp = [];
    list = [];
    addItem('a');
    addItem('b');
    addItem('c');

    service.init(
      () => of(apiResp),
      undefined,
      (updatedList) => (list = updatedList),
      () => true,
      (task) => task.name.startsWith('test'),
      (item, task) => item.name === task.metadata['name'],
      {
        default: (metadata: object) => ({ name: metadata['name'] })
      }
    );
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  const addTask = (name: string, itemName: string, progress?: number) => {
    const task = new ExecutingTask();
    task.name = name;
    task.progress = progress;
    task.metadata = { name: itemName };
    tasks.push(task);
    summaryService.addRunningTask(task);
  };

  it('gets all items without any executing items', () => {
    expect(list.length).toBe(3);
    expect(list.every((item) => !item.cdExecuting)).toBeTruthy();
  });

  it('gets an item from a task during creation', () => {
    addTask('test/create', 'd');
    expect(list.length).toBe(4);
    expectItemTasks(list[3], 'Creating');
  });

  it('shows progress of current task if any above 0', () => {
    addTask('test/edit', 'd', 97);
    addTask('test/edit', 'e', 0);
    expect(list.length).toBe(5);
    expectItemTasks(list[3], 'Updating', 97);
    expectItemTasks(list[4], 'Updating');
  });

  it('gets all items with one executing items', () => {
    addTask('test/create', 'a');
    expect(list.length).toBe(3);
    expectItemTasks(list[0], 'Creating');
    expectItemTasks(list[1], undefined);
    expectItemTasks(list[2], undefined);
  });

  it('gets all items with multiple executing items', () => {
    addTask('test/create', 'a');
    addTask('test/edit', 'a');
    addTask('test/delete', 'a');
    addTask('test/edit', 'b');
    addTask('test/delete', 'b');
    addTask('test/delete', 'c');
    expect(list.length).toBe(3);
    expectItemTasks(list[0], 'Creating..., Updating..., Deleting');
    expectItemTasks(list[1], 'Updating..., Deleting');
    expectItemTasks(list[2], 'Deleting');
  });

  it('gets all items with multiple executing tasks (not only item tasks', () => {
    addTask('rbd/create', 'a');
    addTask('rbd/edit', 'a');
    addTask('test/delete', 'a');
    addTask('test/edit', 'b');
    addTask('rbd/delete', 'b');
    addTask('rbd/delete', 'c');
    expect(list.length).toBe(3);
    expectItemTasks(list[0], 'Deleting');
    expectItemTasks(list[1], 'Updating');
    expectItemTasks(list[2], undefined);
  });

  it('should call ngOnDestroy', () => {
    expect(service.summaryDataSubscription.closed).toBeFalsy();
    service.ngOnDestroy();
    expect(service.summaryDataSubscription.closed).toBeTruthy();
  });
});
