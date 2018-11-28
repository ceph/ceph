import { HttpClientTestingModule } from '@angular/common/http/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import * as _ from 'lodash';
import { PopoverModule } from 'ngx-bootstrap/popover';
import { of } from 'rxjs';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { HealthService } from '../../../shared/api/health.service';
import { SharedModule } from '../../../shared/shared.module';
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
    health: { status: 'HEALTH_OK' },
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
    imports: [SharedModule, HttpClientTestingModule, PopoverModule.forRoot()],
    declarations: [
      HealthComponent,
      MonSummaryPipe,
      OsdSummaryPipe,
      MdsSummaryPipe,
      MgrSummaryPipe,
      PgStatusStylePipe,
      PgStatusPipe
    ],
    schemas: [NO_ERRORS_SCHEMA],
    providers: i18nProviders
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(HealthComponent);
    component = fixture.componentInstance;
    getHealthSpy = spyOn(TestBed.get(HealthService), 'getMinimalHealth');
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

  it('should render all except "Status" group and cards', () => {
    const payload = _.cloneDeep(healthPayload);
    payload.health.status = '';
    payload.mon_status = null;
    payload.osd_map = null;
    payload.mgr_map = null;
    payload.hosts = null;
    payload.rgw = null;
    payload.fs_map = null;
    payload.iscsi_daemons = null;

    getHealthSpy.and.returnValue(of(payload));
    fixture.detectChanges();

    const infoGroups = fixture.debugElement.nativeElement.querySelectorAll('cd-info-group');
    expect(infoGroups.length).toBe(2);

    const infoCards = fixture.debugElement.nativeElement.querySelectorAll('cd-info-card');
    expect(infoCards.length).toBe(10);
  });

  it('should render all except "Performance" group and cards', () => {
    const payload = _.cloneDeep(healthPayload);
    payload.scrub_status = '';
    payload.client_perf = null;

    getHealthSpy.and.returnValue(of(payload));
    fixture.detectChanges();

    const infoGroups = fixture.debugElement.nativeElement.querySelectorAll('cd-info-group');
    expect(infoGroups.length).toBe(2);

    const infoCards = fixture.debugElement.nativeElement.querySelectorAll('cd-info-card');
    expect(infoCards.length).toBe(13);
  });

  it('should render all except "Capacity" group and cards', () => {
    const payload = _.cloneDeep(healthPayload);
    payload.pools = null;
    payload.df = null;
    payload.pg_info = null;

    getHealthSpy.and.returnValue(of(payload));
    fixture.detectChanges();

    const infoGroups = fixture.debugElement.nativeElement.querySelectorAll('cd-info-group');
    expect(infoGroups.length).toBe(2);

    const infoCards = fixture.debugElement.nativeElement.querySelectorAll('cd-info-card');
    expect(infoCards.length).toBe(13);
  });

  it('should render all groups and 1 card per group', () => {
    const payload = { hosts: 0, scrub_status: 'Inactive', pools: [] };

    getHealthSpy.and.returnValue(of(payload));
    fixture.detectChanges();

    const infoGroups = fixture.debugElement.nativeElement.querySelectorAll('cd-info-group');
    expect(infoGroups.length).toBe(3);

    _.each(infoGroups, (infoGroup) => {
      expect(infoGroup.querySelectorAll('cd-info-card').length).toBe(1);
    });
  });
});
