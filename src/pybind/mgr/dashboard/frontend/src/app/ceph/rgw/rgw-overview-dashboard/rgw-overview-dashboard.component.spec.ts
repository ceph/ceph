import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwOverviewDashboardComponent } from './rgw-overview-dashboard.component';
import { of } from 'rxjs';
import { RgwDaemonService } from '~/app/shared/api/rgw-daemon.service';
import { RgwDaemon } from '../models/rgw-daemon';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { RgwRealmService } from '~/app/shared/api/rgw-realm.service';
import { RgwZonegroupService } from '~/app/shared/api/rgw-zonegroup.service';
import { RgwZoneService } from '~/app/shared/api/rgw-zone.service';
import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { HealthService } from '~/app/shared/api/health.service';
import { CardRowComponent } from '~/app/shared/components/card-row/card-row.component';
import { CardComponent } from '~/app/shared/components/card/card.component';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('RgwOverviewDashboardComponent', () => {
  let component: RgwOverviewDashboardComponent;
  let fixture: ComponentFixture<RgwOverviewDashboardComponent>;
  const daemon: RgwDaemon = {
    id: '8000',
    service_map_id: '4803',
    version: 'ceph version',
    server_hostname: 'ceph',
    realm_name: 'realm1',
    zonegroup_name: 'zg1-realm1',
    zone_name: 'zone1-zg1-realm1',
    default: true,
    port: 80
  };

  const realmList = {
    default_info: '20f61d29-7e45-4418-8e19-b7e962e4860b',
    realms: ['realm2', 'realm1']
  };

  const zonegroupList = {
    default_info: '20f61d29-7e45-4418-8e19-b7e962e4860b',
    zonegroups: ['zg-1', 'zg-2', 'zg-3']
  };

  const zoneList = {
    default_info: '20f61d29-7e45-4418-8e19-b7e962e4860b',
    zones: ['zone4', 'zone5', 'zone6', 'zone7']
  };

  const bucketAndUserList = {
    buckets_count: 2,
    users_count: 2
  };

  const healthData = {
    total_objects: '290',
    total_pool_bytes_used: 9338880
  };

  let listDaemonsSpy: jest.SpyInstance;
  let listZonesSpy: jest.SpyInstance;
  let listZonegroupsSpy: jest.SpyInstance;
  let listRealmsSpy: jest.SpyInstance;
  let listBucketsSpy: jest.SpyInstance;
  let healthDataSpy: jest.SpyInstance;

  configureTestBed({
    declarations: [
      RgwOverviewDashboardComponent,
      CardComponent,
      CardRowComponent,
      DimlessBinaryPipe
    ],
    schemas: [NO_ERRORS_SCHEMA],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    listDaemonsSpy = jest
      .spyOn(TestBed.inject(RgwDaemonService), 'list')
      .mockReturnValue(of([daemon]));
    listRealmsSpy = jest
      .spyOn(TestBed.inject(RgwRealmService), 'list')
      .mockReturnValue(of(realmList));
    listZonegroupsSpy = jest
      .spyOn(TestBed.inject(RgwZonegroupService), 'list')
      .mockReturnValue(of(zonegroupList));
    listZonesSpy = jest.spyOn(TestBed.inject(RgwZoneService), 'list').mockReturnValue(of(zoneList));
    listBucketsSpy = jest
      .spyOn(TestBed.inject(RgwBucketService), 'getTotalBucketsAndUsersLength')
      .mockReturnValue(of(bucketAndUserList));
    healthDataSpy = jest
      .spyOn(TestBed.inject(HealthService), 'getClusterCapacity')
      .mockReturnValue(of(healthData));
    fixture = TestBed.createComponent(RgwOverviewDashboardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should render all cards', () => {
    fixture.detectChanges();
    const dashboardCards = fixture.debugElement.nativeElement.querySelectorAll('cd-card');
    expect(dashboardCards.length).toBe(5);
  });

  it('should get corresponding data into Daemons', () => {
    expect(listDaemonsSpy).toHaveBeenCalled();
    expect(component.rgwDaemonCount).toEqual(1);
  });

  it('should get corresponding data into Realms', () => {
    expect(listRealmsSpy).toHaveBeenCalled();
    expect(component.rgwRealmCount).toEqual(2);
  });

  it('should get corresponding data into Zonegroups', () => {
    expect(listZonegroupsSpy).toHaveBeenCalled();
    expect(component.rgwZonegroupCount).toEqual(3);
  });

  it('should get corresponding data into Zones', () => {
    expect(listZonesSpy).toHaveBeenCalled();
    expect(component.rgwZoneCount).toEqual(4);
  });

  it('should get corresponding data into Buckets', () => {
    expect(listBucketsSpy).toHaveBeenCalled();
    expect(component.rgwBucketCount).toEqual(2);
    expect(component.UserCount).toEqual(2);
  });

  it('should get corresponding data into Objects and capacity', () => {
    expect(healthDataSpy).toHaveBeenCalled();
    expect(component.objectCount).toEqual('290');
    expect(component.totalPoolUsedBytes).toEqual(9338880);
  });
});
