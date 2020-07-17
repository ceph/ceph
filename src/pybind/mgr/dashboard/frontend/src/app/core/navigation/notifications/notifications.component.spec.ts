import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastrModule } from 'ngx-toastr';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { ExecutingTask } from '../../../shared/models/executing-task';
import { SummaryService } from '../../../shared/services/summary.service';
import { SharedModule } from '../../../shared/shared.module';
import { NotificationsComponent } from './notifications.component';

describe('NotificationsComponent', () => {
  let component: NotificationsComponent;
  let fixture: ComponentFixture<NotificationsComponent>;
  let summaryService: SummaryService;

  configureTestBed({
    imports: [HttpClientTestingModule, SharedModule, ToastrModule.forRoot(), RouterTestingModule],
    declarations: [NotificationsComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NotificationsComponent);
    component = fixture.componentInstance;
    summaryService = TestBed.inject(SummaryService);

    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should subscribe and check if there are running tasks', () => {
    expect(component.hasRunningTasks).toBeFalsy();

    const task = new ExecutingTask('task', { name: 'name' });
    summaryService['summaryDataSource'].next({ executing_tasks: [task] });

    expect(component.hasRunningTasks).toBeTruthy();
  });
});
