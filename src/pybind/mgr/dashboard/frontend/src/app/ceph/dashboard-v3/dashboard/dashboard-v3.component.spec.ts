import { HttpClientTestingModule } from '@angular/common/http/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { RouterTestingModule } from '@angular/router/testing';

import _ from 'lodash';
import { ToastrModule } from 'ngx-toastr';
import { BehaviorSubject, of } from 'rxjs';

import { HealthService } from '~/app/shared/api/health.service';
import { PrometheusService } from '~/app/shared/api/prometheus.service';
import { CssHelper } from '~/app/shared/classes/css-helper';
import { AlertmanagerAlert } from '~/app/shared/models/prometheus-alerts';
import { FeatureTogglesService } from '~/app/shared/services/feature-toggles.service';
import { PrometheusAlertService } from '~/app/shared/services/prometheus-alert.service';
import { SummaryService } from '~/app/shared/services/summary.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { PgCategoryService } from '../../shared/pg-category.service';
import { DashboardPieComponent } from '../dashboard-pie/dashboard-pie.component';
import { PgSummaryPipe } from '../pg-summary.pipe';
import { DashboardV3Component } from './dashboard-v3.component';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { AlertClass } from '~/app/shared/enum/health-icon.enum';

export class SummaryServiceMock {
  summaryDataSource = new BehaviorSubject({
    version:
      'ceph version 17.0.0-12222-gcd0cd7cb ' +
      '(b8193bb4cda16ccc5b028c3e1df62bc72350a15d) quincy (dev)'
  });
  summaryData$ = this.summaryDataSource.asObservable();

  subscribe(call: any) {
    return this.summaryData$.subscribe(call);
  }
}

describe('Dashbord Component', () => {
  let component: DashboardV3Component;
  let fixture: ComponentFixture<DashboardV3Component>;
  let healthService: HealthService;
  let orchestratorService: OrchestratorService;
  let getHealthSpy: jasmine.Spy;
  let getAlertsSpy: jasmine.Spy;
  let fakeFeatureTogglesService: jasmine.Spy;

  const healthPayload: Record<string, any> = {
    health: { status: 'HEALTH_OK' },
    mon_status: { monmap: { mons: [] }, quorum: [] },
    osd_map: { osds: [] },
    mgr_map: { standbys: [] },
    hosts: 0,
    rgw: 0,
    fs_map: { filesystems: [], standbys: [] },
    iscsi_daemons: 1,
    client_perf: {},
    scrub_status: 'Inactive',
    pools: [],
    df: { stats: {} },
    pg_info: { object_stats: { num_objects: 1 } }
  };

  const alertsPayload: AlertmanagerAlert[] = [
    {
      labels: {
        alertname: 'CephMgrPrometheusModuleInactive',
        instance: 'ceph2:9283',
        job: 'ceph',
        severity: 'critical'
      },
      annotations: {
        description: 'The mgr/prometheus module at ceph2:9283 is unreachable.',
        summary: 'The mgr/prometheus module is not available'
      },
      startsAt: '2022-09-28T08:23:41.152Z',
      endsAt: '2022-09-28T15:28:01.152Z',
      generatorURL: 'http://prometheus:9090/testUrl',
      status: {
        state: 'active',
        silencedBy: null,
        inhibitedBy: null
      },
      receivers: ['ceph2'],
      fingerprint: 'fingerprint'
    },
    {
      labels: {
        alertname: 'CephOSDDownHigh',
        instance: 'ceph:9283',
        job: 'ceph',
        severity: 'critical'
      },
      annotations: {
        description: '66.67% or 2 of 3 OSDs are down (>= 10%).',
        summary: 'More than 10% of OSDs are down'
      },
      startsAt: '2022-09-28T14:17:22.665Z',
      endsAt: '2022-09-28T15:28:32.665Z',
      generatorURL: 'http://prometheus:9090/testUrl',
      status: {
        state: 'active',
        silencedBy: null,
        inhibitedBy: null
      },
      receivers: ['default'],
      fingerprint: 'fingerprint'
    },
    {
      labels: {
        alertname: 'CephHealthWarning',
        instance: 'ceph:9283',
        job: 'ceph',
        severity: 'warning'
      },
      annotations: {
        description: 'The cluster state has been HEALTH_WARN for more than 15 minutes.',
        summary: 'Ceph is in the WARNING state'
      },
      startsAt: '2022-09-28T08:41:38.454Z',
      endsAt: '2022-09-28T15:28:38.454Z',
      generatorURL: 'http://prometheus:9090/testUrl',
      status: {
        state: 'active',
        silencedBy: null,
        inhibitedBy: null
      },
      receivers: ['ceph'],
      fingerprint: 'fingerprint'
    }
  ];

  const configValueData: any = 'e90a0d58-658e-4148-8f61-e896c86f0696';

  const orchName: any = 'Cephadm';

  configureTestBed({
    imports: [RouterTestingModule, HttpClientTestingModule, ToastrModule.forRoot(), SharedModule],
    declarations: [DashboardV3Component, DashboardPieComponent, PgSummaryPipe],
    schemas: [NO_ERRORS_SCHEMA],
    providers: [
      { provide: SummaryService, useClass: SummaryServiceMock },
      PrometheusAlertService,
      CssHelper,
      PgCategoryService
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
    fixture = TestBed.createComponent(DashboardV3Component);
    component = fixture.componentInstance;
    healthService = TestBed.inject(HealthService);
    orchestratorService = TestBed.inject(OrchestratorService);
    getHealthSpy = spyOn(TestBed.inject(HealthService), 'getMinimalHealth');
    getHealthSpy.and.returnValue(of(healthPayload));
    getAlertsSpy = spyOn(TestBed.inject(PrometheusService), 'getAlerts');
    getAlertsSpy.and.returnValue(of(alertsPayload));
    component.prometheusAlertService.alerts = alertsPayload;
    component.isAlertmanagerConfigured = true;
    let prometheusAlertService = TestBed.inject(PrometheusAlertService);
    spyOn(prometheusAlertService, 'getAlerts').and.callFake(() => of([]));
    prometheusAlertService.activeCriticalAlerts = 2;
    prometheusAlertService.activeWarningAlerts = 1;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should render all cards', () => {
    fixture.detectChanges();
    const dashboardCards = fixture.debugElement.nativeElement.querySelectorAll('cd-card');
    expect(dashboardCards.length).toBe(5);
  });

  it('should get corresponding data into detailsCardData', () => {
    spyOn(healthService, 'getClusterFsid').and.returnValue(of(configValueData));
    spyOn(orchestratorService, 'getName').and.returnValue(of(orchName));
    component.ngOnInit();
    expect(component.detailsCardData.fsid).toBe('e90a0d58-658e-4148-8f61-e896c86f0696');
    expect(component.detailsCardData.orchestrator).toBe('Cephadm');
    expect(component.detailsCardData.cephVersion).toBe('17.0.0-12222-gcd0cd7cb quincy (dev)');
  });

  it('should check if the respective icon is shown for each status', () => {
    const payload = _.cloneDeep(healthPayload);

    // HEALTH_WARN
    payload.health['status'] = 'HEALTH_WARN';
    payload.health['checks'] = [
      { severity: 'HEALTH_WARN', type: 'WRN', summary: { message: 'fake warning' } }
    ];

    getHealthSpy.and.returnValue(of(payload));
    fixture.detectChanges();
    const clusterStatusCard = fixture.debugElement.query(By.css('cd-card[cardTitle="Status"] i'));
    expect(clusterStatusCard.nativeElement.title).toEqual(`${payload.health.status}`);

    // HEALTH_ERR
    payload.health['status'] = 'HEALTH_ERR';
    payload.health['checks'] = [
      { severity: 'HEALTH_ERR', type: 'ERR', summary: { message: 'fake error' } }
    ];

    getHealthSpy.and.returnValue(of(payload));
    fixture.detectChanges();
    expect(clusterStatusCard.nativeElement.title).toEqual(`${payload.health.status}`);

    // HEALTH_OK
    payload.health['status'] = 'HEALTH_OK';
    payload.health['checks'] = [
      { severity: 'HEALTH_OK', type: 'OK', summary: { message: 'fake success' } }
    ];

    getHealthSpy.and.returnValue(of(payload));
    fixture.detectChanges();
    expect(clusterStatusCard.nativeElement.title).toEqual(`${payload.health.status}`);
  });

  it('should show the actual alert count on each alerts pill', () => {
    fixture.detectChanges();

    const warningAlerts = fixture.debugElement.query(By.css('button[id=warningAlerts] span'));

    const dangerAlerts = fixture.debugElement.query(By.css('button[id=dangerAlerts] span'));

    expect(warningAlerts.nativeElement.textContent).toBe('1');
    expect(dangerAlerts.nativeElement.textContent).toBe('2');
  });

  it('should show the critical alerts window and its content', () => {
    const payload = _.cloneDeep(alertsPayload[0]);
    component.toggleAlertsWindow(AlertClass[0]);
    fixture.detectChanges();

    const cardTitle = fixture.debugElement.query(By.css('.tc_alerts h6.card-title'));

    expect(cardTitle.nativeElement.textContent).toBe(payload.labels.alertname);
    expect(component.alertType).not.toBe('warning');
  });

  it('should show the warning alerts window and its content', () => {
    const payload = _.cloneDeep(alertsPayload[2]);
    component.toggleAlertsWindow(AlertClass.warning);
    fixture.detectChanges();

    const cardTitle = fixture.debugElement.query(By.css('.tc_alerts h6.card-title'));

    expect(cardTitle.nativeElement.textContent).toBe(payload.labels.alertname);
    expect(component.alertType).not.toBe('critical');
  });

  it('should only show the pills when the alerts are not empty', () => {
    spyOn(TestBed.inject(PrometheusAlertService), 'alerts').and.returnValue(0);
    fixture.detectChanges();

    const warningAlerts = fixture.debugElement.query(By.css('button[id=warningAlerts]'));

    const dangerAlerts = fixture.debugElement.query(By.css('button[id=dangerAlerts]'));

    expect(warningAlerts).toBe(null);
    expect(dangerAlerts).toBe(null);
  });

  it('should render "Status" card text that is not clickable', () => {
    fixture.detectChanges();

    const clusterStatusCard = fixture.debugElement.query(By.css('cd-card[cardTitle="Status"]'));
    const clickableContent = clusterStatusCard.query(By.css('.lead.text-primary'));
    expect(clickableContent).toBeNull();
  });

  it('should render "Status" card text that is clickable (popover)', () => {
    const payload = _.cloneDeep(healthPayload);
    payload.health['status'] = 'HEALTH_WARN';
    payload.health['checks'] = [
      { severity: 'HEALTH_WARN', type: 'WRN', summary: { message: 'fake warning' } }
    ];

    getHealthSpy.and.returnValue(of(payload));
    fixture.detectChanges();

    const clusterStatusCard = fixture.debugElement.query(By.css('cd-card[cardTitle="Status"]'));
    const clickableContent = clusterStatusCard.query(By.css('.lead.text-primary'));
    expect(clickableContent).not.toBeNull();
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
      fixture = TestBed.createComponent(DashboardV3Component);
      component = fixture.componentInstance;
    });

    it('should not render items related to disabled features', () => {
      fixture.detectChanges();

      const iscsiCard = fixture.debugElement.query(By.css('li[id=iscsi-item]'));
      const rgwCard = fixture.debugElement.query(By.css('li[id=rgw-item]'));
      const mds = fixture.debugElement.query(By.css('li[id=mds-item]'));

      expect(iscsiCard).toBeFalsy();
      expect(rgwCard).toBeFalsy();
      expect(mds).toBeFalsy();
    });
  });
});
