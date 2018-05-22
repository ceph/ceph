import { NO_ERRORS_SCHEMA } from '@angular/core';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import 'rxjs/add/observable/of';
import { Observable } from 'rxjs/Observable';

import { PoolService } from '../../../shared/api/pool.service';
import { RbdService } from '../../../shared/api/rbd.service';
import { DimlessBinaryPipe } from '../../../shared/pipes/dimless-binary.pipe';
import { FormatterService } from '../../../shared/services/formatter.service';
import { NotificationService } from '../../../shared/services/notification.service';
import { TaskManagerMessageService } from '../../../shared/services/task-manager-message.service';
import { TaskManagerService } from '../../../shared/services/task-manager.service';
import { RbdFormComponent } from './rbd-form.component';

describe('RbdFormComponent', () => {
  let component: RbdFormComponent;
  let fixture: ComponentFixture<RbdFormComponent>;

  const fakeService = {
    subscribe: (name, metadata, onTaskFinished: (finishedTask: any) => any) => {
      return null;
    },
    defaultFeatures: () => Observable.of([]),
    list: (attrs = []) => {
      return new Promise(function(resolve, reject) {
        return;
      });
    }
  };

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [ReactiveFormsModule, RouterTestingModule],
      declarations: [RbdFormComponent],
      schemas: [NO_ERRORS_SCHEMA],
      providers: [
        DimlessBinaryPipe,
        FormatterService,
        TaskManagerMessageService,
        { provide: NotificationService, useValue: fakeService },
        { provide: PoolService, useValue: fakeService },
        { provide: RbdService, useValue: fakeService },
        { provide: TaskManagerService, useValue: fakeService }
      ]
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(RbdFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
