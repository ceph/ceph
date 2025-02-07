import { HttpClientTestingModule } from '@angular/common/http/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';

import _ from 'lodash';
import { of } from 'rxjs';

import { PgCategoryService } from '~/app/ceph/shared/pg-category.service';
import { HealthService } from '~/app/shared/api/health.service';
import { CssHelper } from '~/app/shared/classes/css-helper';
import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { FeatureTogglesService } from '~/app/shared/services/feature-toggles.service';
import { RefreshIntervalService } from '~/app/shared/services/refresh-interval.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { HealthPieComponent } from '../health-pie/health-pie.component';
import { MdsDashboardSummaryPipe } from '../mds-dashboard-summary.pipe';
import { MgrDashboardSummaryPipe } from '../mgr-dashboard-summary.pipe';
import { MonSummaryPipe } from '../mon-summary.pipe';
import { osdDashboardSummaryPipe } from '../osd-dashboard-summary.pipe';
import { HealthComponent } from './health.component';

describe('HealthComponent', () => {
  let component: HealthComponent;
  let fixture: ComponentFixture<HealthComponent>;
  let getHealthSpy: jasmine.Spy;
  const healthPayload: Record<string, any> = {
    health: { status: 'HEALTH_OK' },
    mon_status: { monmap: { mons: [] }, quorum: [] },
    osd_map: { osds: [] },
    mgr_map: { standbys: [] },
    hosts: 0,
    rgw: 0,
    fs_map: { filesystems: [], standbys: [] },
    iscsi_daemons: 0,
    client_perf: {},
    scrub_status: 'Inactive',
    pools: [],
    df: { stats: {} },
    pg_info: { object_stats: { num_objects: 0 } }
  };
  const fakeAuthStorageService = {
    getPermissions: () => {
      return new Permissions({ log: ['read'] });
    }
  };
  let fakeFeatureTogglesService: jasmine.Spy;

  configureTestBed({
    imports: [SharedModule, HttpClientTestingModule],
    declarations: [
      HealthComponent,
      HealthPieComponent,
      MonSummaryPipe,
      osdDashboardSummaryPipe,
      MdsDashboardSummaryPipe,
      MgrDashboardSummaryPipe
    ],
    schemas: [NO_ERRORS_SCHEMA],
    providers: [
      { provide: AuthStorageService, useValue: fakeAuthStorageService },
      PgCategoryService,
      RefreshIntervalService,
      CssHelper
    ]
  });

  beforeEach(() => {
    fakeFeatureTogglesService = spyOn(TestBed.inject(FeatureTogglesService), 'get').and.returnValue(
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
    getHealthSpy = spyOn(TestBed.inject(HealthService), 'getMinimalHealth');
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
    expect(infoCards.length).toBe(17);
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
      expect(infoCards.length).toBe(14);
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
    expect(infoCards.length).toBe(9);
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
    expect(infoCards.length).toBe(12);
  });

  it('should render all groups and 1 card per group', () => {
    const payload: Record<string, any> = { hosts: 0, scrub_status: 'Inactive', pools: [] };

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
    expect(clusterStatusCard.nativeElement.textContent).toEqual(' OK ');
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
    expect(clickableContent.nativeElement.textContent).toEqual(' WARNING ');
  });

  it('event binding "prepareReadWriteRatio" is called', () => {
    const prepareReadWriteRatio = spyOn(component, 'prepareReadWriteRatio').and.callThrough();

    const payload = _.cloneDeep(healthPayload);
    payload.client_perf['read_op_per_sec'] = 1;
    payload.client_perf['write_op_per_sec'] = 3;
    getHealthSpy.and.returnValue(of(payload));
    fixture.detectChanges();

    expect(prepareReadWriteRatio).toHaveBeenCalled();
    expect(prepareReadWriteRatio.calls.mostRecent().args[0].dataset[0].data).toEqual([25, 75]);
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

  it('event binding "prepareObjects" is called', () => {
    const prepareObjects = spyOn(component, 'prepareObjects');

    fixture.detectChanges();

    expect(prepareObjects).toHaveBeenCalled();
  });

  describe('preparePgStatus', () => {
    const expectedChart = (data: number[], label: string = null) => ({
      labels: [
        `Clean: ${component['dimless'].transform(data[0])}`,
        `Working: ${component['dimless'].transform(data[1])}`,
        `Warning: ${component['dimless'].transform(data[2])}`,
        `Unknown: ${component['dimless'].transform(data[3])}`
      ],
      options: {},
      dataset: [
        {
          data: data.map((i) =>
            component['calcPercentage'](
              i,
              data.reduce((j, k) => j + k)
            )
          ),
          label: label
        }
      ]
    });

    it('gets no data', () => {
      const chart = { dataset: [{}], options: {} };
      component.preparePgStatus(chart, {
        pg_info: {}
      });
      expect(chart).toEqual(expectedChart([0, 0, 0, 0], '0\nPGs'));
    });

    it('gets data from all categories', () => {
      const chart = { dataset: [{}], options: {} };
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
      expect(chart).toEqual(expectedChart([1, 2, 3, 4], '10\nPGs'));
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

  describe('calcPercentage', () => {
    it('returns correct value', () => {
      expect(component['calcPercentage'](1, undefined)).toEqual(0);
      expect(component['calcPercentage'](1, null)).toEqual(0);
      expect(component['calcPercentage'](1, 0)).toEqual(0);
      expect(component['calcPercentage'](undefined, 1)).toEqual(0);
      expect(component['calcPercentage'](null, 1)).toEqual(0);
      expect(component['calcPercentage'](0, 1)).toEqual(0);
      expect(component['calcPercentage'](1, 100000)).toEqual(0.01);
      expect(component['calcPercentage'](2.346, 10)).toEqual(23.46);
      expect(component['calcPercentage'](2.56, 10)).toEqual(25.6);
    });
  });
});
