import { ComponentFixture, TestBed } from '@angular/core/testing';

import { UpgradeComponent } from './upgrade.component';
import { configureTestBed } from '~/testing/unit-test-helper';
import { SummaryService } from '~/app/shared/services/summary.service';
import { BehaviorSubject, of } from 'rxjs';
import { UpgradeService } from '~/app/shared/api/upgrade.service';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { UpgradeInfoInterface } from '~/app/shared/models/upgrade.interface';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { HealthService } from '~/app/shared/api/health.service';
import { SharedModule } from '~/app/shared/shared.module';

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
    imports: [HttpClientTestingModule, SharedModule],
    schemas: [NO_ERRORS_SCHEMA],
    declarations: [UpgradeComponent],
    providers: [UpgradeService, { provide: SummaryService, useClass: SummaryServiceMock }]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(UpgradeComponent);
    component = fixture.componentInstance;
    upgradeInfoSpy = spyOn(TestBed.inject(UpgradeService), 'list');
    getHealthSpy = spyOn(TestBed.inject(HealthService), 'getMinimalHealth');
    getHealthSpy.and.returnValue(of(healthPayload));
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should load the view once check for upgrade is done', () => {
    const upgradeInfoPayload = {
      image: 'quay.io/ceph-test/ceph',
      registry: 'quay.io',
      versions: ['18.1.0', '18.1.1', '18.1.2']
    };
    upgradeInfoSpy.and.returnValue(of(upgradeInfoPayload));
    component.ngOnInit();
    fixture.detectChanges();
    const firstCellSpan = fixture.debugElement.nativeElement.querySelector('span');
    expect(firstCellSpan.textContent).toBe('Current Version');
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
    const loading = fixture.debugElement.nativeElement.querySelector('h3');
    expect(loading.textContent).toBe('Checking for upgrades ');
  });

  it('should upgrade only when there are more than 1 mgr', () => {
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
});
