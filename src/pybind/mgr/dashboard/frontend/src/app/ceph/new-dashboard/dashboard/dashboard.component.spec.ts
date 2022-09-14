import { HttpClientTestingModule } from '@angular/common/http/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import _ from 'lodash';
import { ToastrModule } from 'ngx-toastr';
import { RouterTestingModule } from '@angular/router/testing';

import { BehaviorSubject, of } from 'rxjs';

import { ConfigurationService } from '~/app/shared/api/configuration.service';
import { HealthService } from '~/app/shared/api/health.service';
import { MgrModuleService } from '~/app/shared/api/mgr-module.service';
import { CssHelper } from '~/app/shared/classes/css-helper';
import { PipesModule } from '~/app/shared/pipes/pipes.module';
import { NotificationService } from '~/app/shared/services/notification.service';
import { SummaryService } from '~/app/shared/services/summary.service';
import { configureTestBed } from '~/testing/unit-test-helper';
import { CardComponent } from '../card/card.component';
import { DashboardPieComponent } from '../dashboard-pie/dashboard-pie.component';
import { DashboardComponent } from './dashboard.component';

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

describe('CardComponent', () => {
  let component: DashboardComponent;
  let fixture: ComponentFixture<DashboardComponent>;
  let configurationService: ConfigurationService;
  let orchestratorService: MgrModuleService;
  let getHealthSpy: jasmine.Spy;
  let getNotificationCountSpy: jasmine.Spy;
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

  const notificationCountPayload: Record<string, number> = {
    info: 3,
    error: 4,
    success: 5,
    cephNotifications: 10
  }

  const configValueData: any = {
    value: [
      {
        section: 'mgr',
        value: 'e90a0d58-658e-4148-8f61-e896c86f0696'
      }
    ]
  };

  const orchData: any = {
    log_level: '',
    log_to_cluster: false,
    log_to_cluster_level: 'info',
    log_to_file: false,
    orchestrator: 'cephadm'
  };

  configureTestBed({
    imports: [RouterTestingModule, HttpClientTestingModule, 
      ToastrModule.forRoot(),
      PipesModule],
    declarations: [DashboardComponent, CardComponent, DashboardPieComponent],
    schemas: [NO_ERRORS_SCHEMA],
    providers: [
      { provide: SummaryService, useClass: SummaryServiceMock },
      CssHelper
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(DashboardComponent);
    component = fixture.componentInstance;
    configurationService = TestBed.inject(ConfigurationService);
    orchestratorService = TestBed.inject(MgrModuleService);
    getHealthSpy = spyOn(TestBed.inject(HealthService), 'getMinimalHealth');
    getHealthSpy.and.returnValue(of(healthPayload));
    getNotificationCountSpy = spyOn(TestBed.inject(NotificationService), 'getNotificationCount');
    getNotificationCountSpy.and.returnValue(of(notificationCountPayload));
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should render all cards', () => {
    const dashboardCards = fixture.debugElement.nativeElement.querySelectorAll('cd-card');
    expect(dashboardCards.length).toBe(6);
  });

  it('should get corresponding data into detailsCardData', () => {
    spyOn(configurationService, 'get').and.returnValue(of(configValueData));
    spyOn(orchestratorService, 'getConfig').and.returnValue(of(orchData));
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
    const clusterStatusCard = fixture.debugElement.query(
      By.css('cd-card[title="Status"] i')
    );
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

  it('should show the actual notification count on each notification pill', () => {
    const payload = _.cloneDeep(notificationCountPayload);
    fixture.detectChanges();

    const cephNotification = fixture.debugElement.query(
      By.css('button[id=cephNotification] span')
    );

    const successNotification = fixture.debugElement.query(
      By.css('button[id=successNotification] span')
    );

    const dangerNotification = fixture.debugElement.query(
      By.css('button[id=dangerNotification] span')
    );

    const infoNotification = fixture.debugElement.query(
      By.css('button[id=infoNotification] span')
    );

    expect(cephNotification.nativeElement.textContent).toBe(payload.cephNotifications.toString());
    expect(successNotification.nativeElement.textContent).toBe(payload.success.toString());
    expect(dangerNotification.nativeElement.textContent).toBe(payload.error.toString());
    expect(infoNotification.nativeElement.textContent).toBe(payload.info.toString());
  });
});
