import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterModule } from '@angular/router';
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
    imports: [HttpClientTestingModule, SharedModule, RouterModule.forRoot([])],
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
          public_addrs: { addrvec: [] },
          stats: { num_sessions: [[1, 5]] }
        },
        {
          public_addrs: { addrvec: [] },
          stats: {
            num_sessions: [
              [1, 1],
              [2, 10],
              [3, 1]
            ]
          }
        },
        {
          public_addrs: { addrvec: [] },
          stats: {
            num_sessions: [
              [1, 0],
              [2, 3]
            ]
          }
        },
        {
          public_addrs: { addrvec: [] },
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
    fixture.detectChanges();
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

  it('should normalize monitor public addresses for v1/v2 entries only', () => {
    const normalizePublicAddresses: any = (MonitorComponent as any).publicAddressArray;

    expect(
      normalizePublicAddresses({
        addrvec: [
          { type: 'v2', addr: '10.0.0.1:6789/1234' },
          { type: 'v1', addr: '10.0.0.1:6789/1235' },
          { type: 'msgr2', addr: '10.0.0.1:6789/9999' }
        ]
      })
    ).toEqual(['v1: 10.0.0.1:6789/1235', 'v2: 10.0.0.1:6789/1234']);
  });

  it('should return an empty list when public addresses are missing', () => {
    const normalizePublicAddresses: any = (MonitorComponent as any).publicAddressArray;

    expect(normalizePublicAddresses(undefined)).toEqual([]);
    expect(normalizePublicAddresses({})).toEqual([]);
    expect(normalizePublicAddresses({ addrvec: [] })).toEqual([]);
  });

  it('should render all monitor public addresses in the custom cell template', () => {
    const payload = {
      in_quorum: [
        {
          name: 'mon-a',
          public_addrs: {
            addrvec: [
              { type: 'v2', addr: '10.0.0.1:6789/1234' },
              { type: 'v1', addr: '10.0.0.1:6789/1235' }
            ]
          },
          stats: { num_sessions: [[1, 5]] }
        }
      ],
      mon_status: null,
      out_quorum: []
    };
    getMonitorSpy.and.returnValue(of(payload));

    fixture.detectChanges();
    component.refresh();
    fixture.detectChanges();

    const addresses = fixture.nativeElement.querySelectorAll('[data-testid="address"]');

    expect(addresses.length).toBe(2);
    expect(addresses[0].textContent).toContain('v1: 10.0.0.1:6789/1235');
    expect(addresses[1].textContent).toContain('v2: 10.0.0.1:6789/1234');
  });
});
