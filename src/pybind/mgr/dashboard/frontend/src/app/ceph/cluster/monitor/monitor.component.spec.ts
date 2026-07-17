import { HttpClientTestingModule } from '@angular/common/http/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { of } from 'rxjs';

import { MonitorService } from '~/app/shared/api/monitor.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { MonitorComponent } from './monitor.component';

describe('MonitorComponent', () => {
  let component: MonitorComponent;
  let fixture: ComponentFixture<MonitorComponent>;
  let getMonitorSpy: jasmine.Spy;

  configureTestBed({
    imports: [HttpClientTestingModule, SharedModule],
    declarations: [MonitorComponent],
    schemas: [NO_ERRORS_SCHEMA],
    providers: [MonitorService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(MonitorComponent);
    component = fixture.componentInstance;
    const getMonitorPayload: Record<string, any> = {
      in_quorum: [
        {
          stats: { num_sessions: [[1, 5]] }
        },
        {
          stats: {
            num_sessions: [
              [1, 1],
              [2, 10],
              [3, 1]
            ]
          }
        },
        {
          stats: {
            num_sessions: [
              [1, 0],
              [2, 3]
            ]
          }
        },
        {
          stats: {
            num_sessions: [
              [1, 2],
              [2, 1],
              [3, 7],
              [4, 5]
            ]
          }
        }
      ],
      mon_status: null,
      out_quorum: []
    };
    getMonitorSpy = spyOn(TestBed.inject(MonitorService), 'getMonitor').and.returnValue(
      of(getMonitorPayload)
    );
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should sort by open sessions column correctly', () => {
    component.refresh();

    expect(getMonitorSpy).toHaveBeenCalled();

    expect(component.quorum.columns[4].comparator(undefined, undefined)).toBe(0);
    expect(component.quorum.columns[4].comparator(null, null)).toBe(0);
    expect(component.quorum.columns[4].comparator([], [])).toBe(0);
    expect(
      component.quorum.columns[4].comparator(
        component.quorum.data[0].cdOpenSessions,
        component.quorum.data[3].cdOpenSessions
      )
    ).toBe(0);
    expect(
      component.quorum.columns[4].comparator(
        component.quorum.data[0].cdOpenSessions,
        component.quorum.data[1].cdOpenSessions
      )
    ).toBe(1);
    expect(
      component.quorum.columns[4].comparator(
        component.quorum.data[1].cdOpenSessions,
        component.quorum.data[0].cdOpenSessions
      )
    ).toBe(-1);
    expect(
      component.quorum.columns[4].comparator(
        component.quorum.data[2].cdOpenSessions,
        component.quorum.data[1].cdOpenSessions
      )
    ).toBe(1);
  });
});
