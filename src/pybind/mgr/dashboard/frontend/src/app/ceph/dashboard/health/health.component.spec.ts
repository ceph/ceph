import { HttpClientTestingModule } from '@angular/common/http/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';

import * as _ from 'lodash';
import { PopoverModule } from 'ngx-bootstrap/popover';
import { of } from 'rxjs';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { HealthService } from '../../../shared/api/health.service';
import { Permissions } from '../../../shared/models/permissions';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { FeatureTogglesService } from '../../../shared/services/feature-toggles.service';
import { RefreshIntervalService } from '../../../shared/services/refresh-interval.service';
import { SharedModule } from '../../../shared/shared.module';
import { PgCategoryService } from '../../shared/pg-category.service';
import { HealthPieColor } from '../health-pie/health-pie-color.enum';
import { HealthPieComponent } from '../health-pie/health-pie.component';
import { MdsSummaryPipe } from '../mds-summary.pipe';
import { MgrSummaryPipe } from '../mgr-summary.pipe';
import { MonSummaryPipe } from '../mon-summary.pipe';
import { OsdSummaryPipe } from '../osd-summary.pipe';
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
  const fakeAuthStorageService = {
    getPermissions: () => {
      return new Permissions({ log: ['read'] });
    }
  };
  let fakeFeatureTogglesService;

  configureTestBed({
    imports: [SharedModule, HttpClientTestingModule, PopoverModule.forRoot()],
    declarations: [
      HealthComponent,
      HealthPieComponent,
      MonSummaryPipe,
      OsdSummaryPipe,
      MdsSummaryPipe,
      MgrSummaryPipe
    ],
    schemas: [NO_ERRORS_SCHEMA],
    providers: [
      i18nProviders,
      { provide: AuthStorageService, useValue: fakeAuthStorageService },
      PgCategoryService,
      RefreshIntervalService
    ]
  });

  beforeEach(() => {
    fakeFeatureTogglesService = spyOn(TestBed.get(FeatureTogglesService), 'get').and.returnValue(
      of({
        rbd: true,
        mirroring: true,
        iscsi: true,
        cephfs: true,
        rgw: true
      })
    );
    fixture = TestBed.createComponent(HealthComponent);
    component = fixture.componentInstance;
    getHealthSpy = spyOn(TestBed.get(HealthService), 'getMinimalHealth');
    getHealthSpy.and.returnValue(of(healthPayload));
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should render all info groups and all info cards', () => {
    fixture.detectChanges();

    const infoGroups = fixture.debugElement.nativeElement.querySelectorAll('cd-info-group');
    expect(infoGroups.length).toBe(3);

    const infoCards = fixture.debugElement.nativeElement.querySelectorAll('cd-info-card');
    expect(infoCards.length).toBe(18);
  });

  describe('features disabled', () => {
    beforeEach(() => {
      fakeFeatureTogglesService.and.returnValue(
        of({
          rbd: false,
          mirroring: false,
          iscsi: false,
          cephfs: false,
          rgw: false
        })
      );
      fixture = TestBed.createComponent(HealthComponent);
      component = fixture.componentInstance;
    });

    it('should not render cards related to disabled features', () => {
      fixture.detectChanges();

      const infoGroups = fixture.debugElement.nativeElement.querySelectorAll('cd-info-group');
      expect(infoGroups.length).toBe(3);

      const infoCards = fixture.debugElement.nativeElement.querySelectorAll('cd-info-card');
      expect(infoCards.length).toBe(15);
    });
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

  it('should render "Cluster Status" card text that is not clickable', () => {
    fixture.detectChanges();

    const clusterStatusCard = fixture.debugElement.query(
      By.css('cd-info-card[cardTitle="Cluster Status"]')
    );
    const clickableContent = clusterStatusCard.query(By.css('.info-card-content-clickable'));
    expect(clickableContent).toBeNull();
    expect(clusterStatusCard.nativeElement.textContent).toEqual(` ${healthPayload.health.status} `);
  });

  it('should render "Cluster Status" card text that is clickable (popover)', () => {
    const payload = _.cloneDeep(healthPayload);
    payload.health['status'] = 'HEALTH_WARN';
    payload.health['checks'] = [
      { severity: 'HEALTH_WARN', type: 'WRN', summary: { message: 'fake warning' } }
    ];

    getHealthSpy.and.returnValue(of(payload));
    fixture.detectChanges();

    expect(component.permissions.log.read).toBeTruthy();

    const clusterStatusCard = fixture.debugElement.query(
      By.css('cd-info-card[cardTitle="Cluster Status"]')
    );
    const clickableContent = clusterStatusCard.query(By.css('.info-card-content-clickable'));
    expect(clickableContent.nativeElement.textContent).toEqual(` ${payload.health.status} `);
  });

  it('event binding "prepareReadWriteRatio" is called', () => {
    const prepareReadWriteRatio = spyOn(component, 'prepareReadWriteRatio');

    const payload = _.cloneDeep(healthPayload);
    payload.client_perf['read_op_per_sec'] = 1;
    payload.client_perf['write_op_per_sec'] = 1;
    getHealthSpy.and.returnValue(of(payload));
    fixture.detectChanges();

    expect(prepareReadWriteRatio).toHaveBeenCalled();
  });

  it('event binding "prepareRawUsage" is called', () => {
    const prepareRawUsage = spyOn(component, 'prepareRawUsage');

    fixture.detectChanges();

    expect(prepareRawUsage).toHaveBeenCalled();
  });

  it('event binding "preparePgStatus" is called', () => {
    const preparePgStatus = spyOn(component, 'preparePgStatus');

    fixture.detectChanges();

    expect(preparePgStatus).toHaveBeenCalled();
  });

  describe('preparePgStatus', () => {
    const expectedChart = (data: number[]) => ({
      colors: [
        {
          backgroundColor: [
            HealthPieColor.DEFAULT_GREEN,
            HealthPieColor.DEFAULT_BLUE,
            HealthPieColor.DEFAULT_ORANGE,
            HealthPieColor.DEFAULT_RED
          ]
        }
      ],
      labels: ['Clean', 'Working', 'Warning', 'Unknown'],
      dataset: [{ data: data }]
    });

    it('gets no data', () => {
      const chart = { dataset: [{}] };
      component.preparePgStatus(chart, { pg_info: {} });
      expect(chart).toEqual(expectedChart([undefined, undefined, undefined, undefined]));
    });

    it('gets data from all categories', () => {
      const chart = { dataset: [{}] };
      component.preparePgStatus(chart, {
        pg_info: {
          statuses: {
            'clean+active+scrubbing+nonMappedState': 4,
            'clean+active+scrubbing': 2,
            'clean+active': 1,
            'clean+active+scrubbing+down': 3
          }
        }
      });
      expect(chart).toEqual(expectedChart([1, 2, 3, 4]));
    });
  });

  describe('isClientReadWriteChartShowable', () => {
    beforeEach(() => {
      component.healthData = healthPayload;
    });

    it('returns false', () => {
      component.healthData['client_perf'] = {};

      expect(component.isClientReadWriteChartShowable()).toBeFalsy();
    });

    it('returns false', () => {
      component.healthData['client_perf'] = { read_op_per_sec: undefined, write_op_per_sec: 0 };

      expect(component.isClientReadWriteChartShowable()).toBeFalsy();
    });

    it('returns true', () => {
      component.healthData['client_perf'] = { read_op_per_sec: 1, write_op_per_sec: undefined };

      expect(component.isClientReadWriteChartShowable()).toBeTruthy();
    });

    it('returns true', () => {
      component.healthData['client_perf'] = { read_op_per_sec: 2, write_op_per_sec: 3 };

      expect(component.isClientReadWriteChartShowable()).toBeTruthy();
    });
  });
});
