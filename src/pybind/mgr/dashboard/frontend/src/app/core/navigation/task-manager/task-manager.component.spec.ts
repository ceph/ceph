import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { PopoverModule } from 'ngx-bootstrap';

import { ExecutingTask } from '../../../shared/models/executing-task';
import { FinishedTask } from '../../../shared/models/finished-task';
import { SharedModule } from '../../../shared/shared.module';
import { configureTestBed } from '../../../shared/unit-test-helper';
import { TaskManagerComponent } from './task-manager.component';

describe('TaskManagerComponent', () => {
  let component: TaskManagerComponent;
  let fixture: ComponentFixture<TaskManagerComponent>;
  const tasks = {
    executing: [],
    finished: []
  };

  configureTestBed({
    imports: [SharedModule, PopoverModule.forRoot(), HttpClientTestingModule],
    declarations: [TaskManagerComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(TaskManagerComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
    tasks.executing = [
      new ExecutingTask('rbd/delete', {
        pool_name: 'somePool',
        image_name: 'someImage'
      })
    ];
    tasks.finished = [
      new FinishedTask('rbd/copy', {
        dest_pool_name: 'somePool',
        dest_image_name: 'someImage'
      }),
      new FinishedTask('rbd/clone', {
        child_pool_name: 'somePool',
        child_image_name: 'someImage'
      })
    ];
    tasks.finished[1].success = false;
    tasks.finished[1].exception = { code: 17 };
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should get executing message for task', () => {
    component._handleTasks(tasks.executing, []);
    expect(component.executingTasks.length).toBe(1);
    expect(component.executingTasks[0].description).toBe('Deleting RBD \'somePool/someImage\'');
  });

  it('should get finished message for task', () => {
    component._handleTasks([], tasks.finished);
    expect(component.finishedTasks.length).toBe(2);
    expect(component.finishedTasks[0].description).toBe('Copy RBD \'somePool/someImage\'');
    expect(component.finishedTasks[0].errorMessage).toBe(undefined);
    expect(component.finishedTasks[1].description).toBe('Clone RBD \'somePool/someImage\'');
    expect(component.finishedTasks[1].errorMessage).toBe(
      'Name \'somePool/someImage\' is already in use.'
    );
  });

  it('should get an empty hour glass with only finished tasks', () => {
    component._setIcon(0);
    expect(component.icon).toBe('fa-hourglass-o');
  });

  it('should get a nearly empty hour glass with executing tasks', () => {
    component._setIcon(10);
    expect(component.icon).toBe('fa-hourglass-start');
  });
});
