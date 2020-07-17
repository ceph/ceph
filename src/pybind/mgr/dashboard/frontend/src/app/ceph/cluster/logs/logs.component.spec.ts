import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';

import { NgbDatepickerModule, NgbNavModule, NgbTimepickerModule } from '@ng-bootstrap/ng-bootstrap';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { SharedModule } from '../../../shared/shared.module';
import { LogsComponent } from './logs.component';

describe('LogsComponent', () => {
  let component: LogsComponent;
  let fixture: ComponentFixture<LogsComponent>;

  configureTestBed({
    imports: [
      HttpClientTestingModule,
      NgbNavModule,
      SharedModule,
      FormsModule,
      NgbDatepickerModule,
      NgbTimepickerModule
    ],
    declarations: [LogsComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(LogsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('abstractfilters', () => {
    it('after initializaed', () => {
      const filters = component.abstractfilters();
      expect(filters.priority).toBe('All');
      expect(filters.key).toBe('');
      expect(filters.yearMonthDay).toBe('');
      expect(filters.sTime).toBe(0);
      expect(filters.eTime).toBe(1439);
    });
    it('change date', () => {
      component.selectedDate = { year: 2019, month: 1, day: 1 };
      component.startTime = { hour: 1, minute: 10 };
      component.endTime = { hour: 12, minute: 10 };
      const filters = component.abstractfilters();
      expect(filters.yearMonthDay).toBe('2019-01-01');
      expect(filters.sTime).toBe(70);
      expect(filters.eTime).toBe(730);
    });
  });

  describe('filterLogs', () => {
    const contentData: Record<string, any> = {
      clog: [
        {
          name: 'priority',
          stamp: '2019-02-21 09:39:49.572801',
          message: 'Manager daemon localhost is now available',
          priority: '[ERR]'
        },
        {
          name: 'search',
          stamp: '2019-02-21 09:39:49.572801',
          message: 'Activating manager daemon localhost',
          priority: '[INF]'
        },
        {
          name: 'date',
          stamp: '2019-01-21 09:39:49.572801',
          message: 'Manager daemon localhost is now available',
          priority: '[INF]'
        },
        {
          name: 'time',
          stamp: '2019-02-21 01:39:49.572801',
          message: 'Manager daemon localhost is now available',
          priority: '[INF]'
        }
      ],
      audit_log: []
    };
    const resetFilter = () => {
      component.selectedDate = null;
      component.priority = 'All';
      component.search = '';
      component.startTime = { hour: 0, minute: 0 };
      component.endTime = { hour: 23, minute: 59 };
    };
    beforeEach(() => {
      component.contentData = contentData;
    });

    it('show all log', () => {
      component.filterLogs();
      expect(component.clog.length).toBe(4);
    });

    it('filter by search key', () => {
      resetFilter();
      component.search = 'Activating';
      component.filterLogs();
      expect(component.clog.length).toBe(1);
      expect(component.clog[0].name).toBe('search');
    });

    it('filter by date', () => {
      resetFilter();
      component.selectedDate = { year: 2019, month: 1, day: 21 };
      component.filterLogs();
      expect(component.clog.length).toBe(1);
      expect(component.clog[0].name).toBe('date');
    });

    it('filter by priority', () => {
      resetFilter();
      component.priority = '[ERR]';
      component.filterLogs();
      expect(component.clog.length).toBe(1);
      expect(component.clog[0].name).toBe('priority');
    });

    it('filter by time range', () => {
      resetFilter();
      component.startTime = { hour: 1, minute: 0 };
      component.endTime = { hour: 2, minute: 0 };
      component.filterLogs();
      expect(component.clog.length).toBe(1);
      expect(component.clog[0].name).toBe('time');
    });
  });
});
