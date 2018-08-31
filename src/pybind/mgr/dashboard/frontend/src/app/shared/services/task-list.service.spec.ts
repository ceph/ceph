import { HttpClientTestingModule } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { of } from 'rxjs';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { ExecutingTask } from '../models/executing-task';
import { SummaryService } from './summary.service';
import { TaskListService } from './task-list.service';
import { TaskMessageService } from './task-message.service';

describe('TaskListService', () => {
  let service: TaskListService;
  let summaryService: SummaryService;
  let taskMessageService: TaskMessageService;

  let reset: boolean;
  let list: any[];
  let apiResp: any;
  let tasks: any[];

  const addItem = (name) => {
    apiResp.push({ name: name });
  };

  configureTestBed({
    providers: [TaskListService, TaskMessageService, SummaryService],
    imports: [HttpClientTestingModule, RouterTestingModule]
  });

  beforeEach(() => {
    service = TestBed.get(TaskListService);
    summaryService = TestBed.get(SummaryService);
    taskMessageService = TestBed.get(TaskMessageService);
    summaryService['summaryDataSource'].next({ executing_tasks: [] });

    taskMessageService.messages['test/create'] = taskMessageService.messages['rbd/create'];
    taskMessageService.messages['test/edit'] = taskMessageService.messages['rbd/edit'];
    taskMessageService.messages['test/delete'] = taskMessageService.messages['rbd/delete'];

    reset = false;
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
      () => (reset = true),
      (task) => task.name.startsWith('test'),
      (item, task) => item.name === task.metadata['name'],
      {
        default: (task) => ({ name: task.metadata['name'] })
      }
    );
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  const addTask = (name: string, itemName: string) => {
    const task = new ExecutingTask();
    task.name = name;
    task.metadata = { name: itemName };
    tasks.push(task);
    summaryService.addRunningTask(task);
  };

  const expectItemTasks = (item: any, executing: string) => {
    expect(item.cdExecuting).toBe(executing);
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
    expectItemTasks(list[0], 'Creating, Updating, Deleting');
    expectItemTasks(list[1], 'Updating, Deleting');
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
