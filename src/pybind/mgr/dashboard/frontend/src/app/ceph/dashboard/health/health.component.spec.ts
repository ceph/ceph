import { HttpClientTestingModule } from '@angular/common/http/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { PopoverModule } from 'ngx-bootstrap/popover';
import { of } from 'rxjs';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { DashboardService } from '../../../shared/api/dashboard.service';
import { SharedModule } from '../../../shared/shared.module';
import { LogColorPipe } from '../log-color.pipe';
import { MdsSummaryPipe } from '../mds-summary.pipe';
import { MgrSummaryPipe } from '../mgr-summary.pipe';
import { MonSummaryPipe } from '../mon-summary.pipe';
import { OsdSummaryPipe } from '../osd-summary.pipe';
import { PgStatusStylePipe } from '../pg-status-style.pipe';
import { PgStatusPipe } from '../pg-status.pipe';
import { HealthComponent } from './health.component';

describe('HealthComponent', () => {
  let component: HealthComponent;
  let fixture: ComponentFixture<HealthComponent>;
  let getHealthSpy;
  const healthPayload = {
    health: { status: 'HEALTH_OK', checks: [] },
    mon_status: { monmap: { mons: [] }, quorum: [] },
    osd_map: { osds: [] },
    mgr_map: { standbys: [] },
    hosts: 0,
    rgw: 0,
    fs_map: { filesystems: [] },
    iscsi_daemons: 0,
    client_perf: {},
    scrub_status: 'Inactive',
    pools: [],
    df: { stats: { total_objects: 0 } },
    pg_info: {}
  };

  configureTestBed({
    providers: [DashboardService],
    imports: [SharedModule, HttpClientTestingModule, PopoverModule.forRoot()],
    declarations: [
      HealthComponent,
      MonSummaryPipe,
      OsdSummaryPipe,
      MdsSummaryPipe,
      MgrSummaryPipe,
      PgStatusStylePipe,
      LogColorPipe,
      PgStatusPipe
    ],
    schemas: [NO_ERRORS_SCHEMA]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(HealthComponent);
    component = fixture.componentInstance;
    getHealthSpy = spyOn(fixture.debugElement.injector.get(DashboardService), 'getHealth');
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should render all info groups and all info cards', () => {
    getHealthSpy.and.returnValue(of(healthPayload));
    fixture.detectChanges();

    const infoGroups = fixture.debugElement.nativeElement.querySelectorAll('cd-info-group');
    expect(infoGroups.length).toBe(3);

    const infoCards = fixture.debugElement.nativeElement.querySelectorAll('cd-info-card');
    expect(infoCards.length).toBe(18);
  });

  // @TODO: remove this test when logs are no longer in landing page
  // See https://tracker.ceph.com/issues/24571 & https://github.com/ceph/ceph/pull/23834
  it('should render Logs group & cards in addition to the other ones', () => {
    const payload = healthPayload;
    payload['clog'] = [];
    payload['audit_log'] = [];

    getHealthSpy.and.returnValue(of(payload));
    fixture.detectChanges();

    const infoGroups = fixture.debugElement.nativeElement.querySelectorAll('cd-info-group');
    expect(infoGroups.length).toBe(4);

    const infoCards = fixture.debugElement.nativeElement.querySelectorAll('cd-info-card');
    expect(infoCards.length).toBe(20);
  });
});
