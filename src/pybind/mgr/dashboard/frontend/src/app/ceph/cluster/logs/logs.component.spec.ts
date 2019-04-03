import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';

import { BsDatepickerModule } from 'ngx-bootstrap/datepicker';
import { TabsModule } from 'ngx-bootstrap/tabs';
import { TimepickerModule } from 'ngx-bootstrap/timepicker';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { SharedModule } from '../../../shared/shared.module';
import { LogsComponent } from './logs.component';

describe('LogsComponent', () => {
  let component: LogsComponent;
  let fixture: ComponentFixture<LogsComponent>;

  configureTestBed({
    imports: [
      HttpClientTestingModule,
      TabsModule.forRoot(),
      SharedModule,
      BsDatepickerModule.forRoot(),
      TimepickerModule.forRoot(),
      FormsModule
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
      component.selectedDate = new Date(2019, 0, 1);
      component.startTime = new Date(2019, 1, 1, 1, 10);
      component.endTime = new Date(2019, 1, 1, 12, 10);
      const filters = component.abstractfilters();
      expect(filters.yearMonthDay).toBe('2019-01-01');
      expect(filters.sTime).toBe(70);
      expect(filters.eTime).toBe(730);
    });
  });

  describe('filterLogs', () => {
    const contentData = {
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
      component.startTime.setHours(0, 0);
      component.endTime.setHours(23, 59);
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
      component.selectedDate = new Date(2019, 0, 21);
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
      component.startTime.setHours(1, 0);
      component.endTime.setHours(2, 0);
      component.filterLogs();
      expect(component.clog.length).toBe(1);
      expect(component.clog[0].name).toBe('time');
    });
  });
});
