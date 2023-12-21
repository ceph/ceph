import { ComponentFixture, TestBed } from '@angular/core/testing';

import { UpgradeComponent } from './upgrade.component';
import { configureTestBed } from '~/testing/unit-test-helper';
import { SummaryService } from '~/app/shared/services/summary.service';
import { BehaviorSubject, of } from 'rxjs';
import { UpgradeService } from '~/app/shared/api/upgrade.service';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { UpgradeInfoInterface } from '~/app/shared/models/upgrade.interface';
import { HealthService } from '~/app/shared/api/health.service';
import { SharedModule } from '~/app/shared/shared.module';
import { LogsComponent } from '../logs/logs.component';
import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ToastrModule } from 'ngx-toastr';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { RouterTestingModule } from '@angular/router/testing';

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

describe('UpgradeComponent', () => {
  let component: UpgradeComponent;
  let fixture: ComponentFixture<UpgradeComponent>;
  let upgradeInfoSpy: jasmine.Spy;
  let getHealthSpy: jasmine.Spy;
  let upgradeStatusSpy: jasmine.Spy;

  const healthPayload: Record<string, any> = {
    health: { status: 'HEALTH_OK' },
    mon_status: { monmap: { mons: [] }, quorum: [] },
    osd_map: { osds: [] },
    mgr_map: { active_name: 'test_mgr', standbys: [] },
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

  configureTestBed({
    imports: [
      HttpClientTestingModule,
      SharedModule,
      NgbNavModule,
      ToastrModule.forRoot(),
      RouterTestingModule
    ],
    declarations: [UpgradeComponent, LogsComponent],
    schemas: [NO_ERRORS_SCHEMA],
    providers: [UpgradeService, { provide: SummaryService, useClass: SummaryServiceMock }]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(UpgradeComponent);
    component = fixture.componentInstance;
    upgradeInfoSpy = spyOn(TestBed.inject(UpgradeService), 'list').and.callFake(() => of(null));
    getHealthSpy = spyOn(TestBed.inject(HealthService), 'getMinimalHealth');
    upgradeStatusSpy = spyOn(TestBed.inject(UpgradeService), 'status');
    getHealthSpy.and.returnValue(of(healthPayload));
    const upgradeInfoPayload = {
      image: 'quay.io/ceph-test/ceph',
      registry: 'quay.io',
      versions: ['18.1.0', '18.1.1', '18.1.2']
    };
    upgradeInfoSpy.and.returnValue(of(upgradeInfoPayload));
    upgradeStatusSpy.and.returnValue(of({}));
    component.fetchStatus();
    spyOn(TestBed.inject(AuthStorageService), 'getPermissions').and.callFake(() => ({
      configOpt: { read: true }
    }));
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should load the view once check for upgrade is done', () => {
    component.ngOnInit();
    fixture.detectChanges();
    const firstCellSpan = fixture.debugElement.nativeElement.querySelector(
      'cd-card[cardTitle="New Version"] .card-title'
    );
    expect(firstCellSpan.textContent).toContain('New Version');
  });

  it('should show button to Upgrade if a new version is available', () => {
    const upgradeInfoPayload = {
      image: 'quay.io/ceph-test/ceph',
      registry: 'quay.io',
      versions: ['18.1.0', '18.1.1', '18.1.2']
    };
    upgradeInfoSpy.and.returnValue(of(upgradeInfoPayload));
    component.ngOnInit();
    fixture.detectChanges();
    const upgradeNowBtn = fixture.debugElement.nativeElement.querySelector('#upgrade');
    expect(upgradeNowBtn).not.toBeNull();
  });

  it('should not show the upgrade button if there are no new version available', () => {
    const upgradeInfoPayload: UpgradeInfoInterface = {
      image: 'quay.io/ceph-test/ceph',
      registry: 'quay.io',
      versions: []
    };
    upgradeInfoSpy.and.returnValue(of(upgradeInfoPayload));
    component.ngOnInit();
    fixture.detectChanges();
    const noUpgradesSpan = fixture.debugElement.nativeElement.querySelector(
      '#no-upgrades-available'
    );
    expect(noUpgradesSpan.textContent).toBe(' Cluster is up-to-date ');
  });

  it('should show the loading screen while the api call is pending', () => {
    upgradeInfoSpy.and.returnValue(of(null));
    component.ngOnInit();
    fixture.detectChanges();
    const loading = fixture.debugElement.nativeElement.querySelector('#newVersionAvailable');
    expect(loading.textContent).toContain('Checking for upgrade');
  });

  it('should upgrade only when there are more than 1 mgr', () => {
    // Only one mgr in payload
    const upgradeInfoPayload = {
      image: 'quay.io/ceph-test/ceph',
      registry: 'quay.io',
      versions: ['18.1.0', '18.1.1', '18.1.2']
    };
    upgradeInfoSpy.and.returnValue(of(upgradeInfoPayload));
    component.ngOnInit();
    fixture.detectChanges();
    const upgradeBtn = fixture.debugElement.nativeElement.querySelector('#upgrade');
    expect(upgradeBtn.disabled).toBeTruthy();

    // Add a standby mgr to the payload
    const healthPayload2: Record<string, any> = {
      health: { status: 'HEALTH_OK' },
      mon_status: { monmap: { mons: [] }, quorum: [] },
      osd_map: { osds: [] },
      mgr_map: { active_name: 'test_mgr', standbys: ['mgr1'] },
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

    getHealthSpy.and.returnValue(of(healthPayload2));
    component.ngOnInit();
    fixture.detectChanges();
    expect(upgradeBtn.disabled).toBeFalsy();
  });

  it('should show the error message when the upgrade fetch fails', () => {
    upgradeInfoSpy.and.returnValue(of(null));
    component.errorMessage = 'Failed to retrieve';
    component.ngOnInit();
    fixture.detectChanges();
    const loading = fixture.debugElement.nativeElement.querySelector('#upgrade-status-error');
    expect(loading.textContent).toContain('Failed to retrieve');
  });

  it('should show popover when health warning is present', () => {
    const healthPayload: Record<string, any> = {
      health: {
        status: 'HEALTH_WARN',
        checks: [
          {
            severity: 'HEALTH_WARN',
            summary: { message: '1 pool(s) do not have an application enabled', count: 1 },
            detail: [
              { message: "application not enabled on pool 'scbench'" },
              {
                message:
                  "use 'ceph osd pool application enable <pool-name> <app-name>', where <app-name> is 'cephfs', 'rbd', 'rgw', or freeform for custom applications."
              }
            ],
            muted: false,
            type: 'POOL_APP_NOT_ENABLED'
          }
        ],
        mutes: []
      }
    };

    getHealthSpy.and.returnValue(of(healthPayload));
    component.ngOnInit();
    fixture.detectChanges();

    const popover = fixture.debugElement.nativeElement.querySelector(
      '.info-card-content-clickable'
    );
    expect(popover).not.toBeNull();
  });

  it('should not show popover when health warning is not present', () => {
    const healthPayload: Record<string, any> = {
      health: {
        status: 'HEALTH_OK'
      }
    };
    getHealthSpy.and.returnValue(of(healthPayload));
    component.ngOnInit();
    fixture.detectChanges();
    const popover = fixture.debugElement.nativeElement.querySelector(
      '.info-card-content-clickable'
    );
    expect(popover).toBeNull();
  });
});
