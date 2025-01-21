import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { of, BehaviorSubject, combineLatest } from 'rxjs';
import { RgwOverviewDashboardComponent } from './rgw-overview-dashboard.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { RgwDaemonService } from '~/app/shared/api/rgw-daemon.service';
import { RgwDaemon } from '../models/rgw-daemon';
import { CardComponent } from '~/app/shared/components/card/card.component';
import { CardRowComponent } from '~/app/shared/components/card-row/card-row.component';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { RgwRealmService } from '~/app/shared/api/rgw-realm.service';
import { RgwZoneService } from '~/app/shared/api/rgw-zone.service';
import { RgwZonegroupService } from '~/app/shared/api/rgw-zonegroup.service';

describe('RgwOverviewDashboardComponent', () => {
  let component: RgwOverviewDashboardComponent;
  let fixture: ComponentFixture<RgwOverviewDashboardComponent>;
  let listDaemonsSpy: jest.SpyInstance;
  let listRealmsSpy: jest.SpyInstance;
  let listZonegroupsSpy: jest.SpyInstance;
  let listZonesSpy: jest.SpyInstance;
  let fetchAndTransformBucketsSpy: jest.SpyInstance;
  let totalBucketsAndUsersSpy: jest.SpyInstance;

  const totalNumObjectsSubject = new BehaviorSubject<number>(290);
  const totalUsedCapacitySubject = new BehaviorSubject<number>(9338880);
  const averageObjectSizeSubject = new BehaviorSubject<number>(1280);
  const bucketsCount = 2;
  const usersCount = 5;
  const daemon: RgwDaemon = {
    id: '8000',
    service_map_id: '4803',
    version: 'ceph version',
    server_hostname: 'ceph',
    realm_name: 'realm1',
    zonegroup_name: 'zg1-realm1',
    zonegroup_id: 'zg1-id',
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

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [
        RgwOverviewDashboardComponent,
        CardComponent,
        CardRowComponent,
        DimlessBinaryPipe
      ],
      schemas: [NO_ERRORS_SCHEMA],
      providers: [
        { provide: RgwDaemonService, useValue: { list: jest.fn() } },
        { provide: RgwRealmService, useValue: { list: jest.fn() } },
        { provide: RgwZonegroupService, useValue: { list: jest.fn() } },
        { provide: RgwZoneService, useValue: { list: jest.fn() } },
        {
          provide: RgwBucketService,
          useValue: {
            fetchAndTransformBuckets: jest.fn(),
            totalNumObjects$: totalNumObjectsSubject.asObservable(),
            totalUsedCapacity$: totalUsedCapacitySubject.asObservable(),
            averageObjectSize$: averageObjectSizeSubject.asObservable(),
            getTotalBucketsAndUsersLength: jest.fn()
          }
        }
      ],
      imports: [HttpClientTestingModule]
    }).compileComponents();
    fixture = TestBed.createComponent(RgwOverviewDashboardComponent);
    component = fixture.componentInstance;
    listDaemonsSpy = jest
      .spyOn(TestBed.inject(RgwDaemonService), 'list')
      .mockReturnValue(of([daemon]));
    fetchAndTransformBucketsSpy = jest
      .spyOn(TestBed.inject(RgwBucketService), 'fetchAndTransformBuckets')
      .mockReturnValue(of(null));
    totalBucketsAndUsersSpy = jest
      .spyOn(TestBed.inject(RgwBucketService), 'getTotalBucketsAndUsersLength')
      .mockReturnValue(of({ buckets_count: bucketsCount, users_count: usersCount }));
    listRealmsSpy = jest
      .spyOn(TestBed.inject(RgwRealmService), 'list')
      .mockReturnValue(of(realmList));
    listZonegroupsSpy = jest
      .spyOn(TestBed.inject(RgwZonegroupService), 'list')
      .mockReturnValue(of(zonegroupList));
    listZonesSpy = jest.spyOn(TestBed.inject(RgwZoneService), 'list').mockReturnValue(of(zoneList));
    fixture.detectChanges();
  });

  it('should create the component', () => {
    expect(component).toBeTruthy();
  });

  it('should render all cards', () => {
    const dashboardCards = fixture.debugElement.nativeElement.querySelectorAll('cd-card');
    expect(dashboardCards.length).toBe(5);
  });

  it('should get data for Realms', () => {
    expect(listRealmsSpy).toHaveBeenCalled();
    expect(component.rgwRealmCount).toEqual(2);
  });

  it('should get data for Zonegroups', () => {
    expect(listZonegroupsSpy).toHaveBeenCalled();
    expect(component.rgwZonegroupCount).toEqual(3);
  });

  it('should get data for Zones', () => {
    expect(listZonesSpy).toHaveBeenCalled();
    expect(component.rgwZoneCount).toEqual(4);
  });

  it('should set component properties from services using combineLatest', fakeAsync(() => {
    component.interval = of(null).subscribe(() => {
      component.fetchDataSub = combineLatest([
        TestBed.inject(RgwDaemonService).list(),
        TestBed.inject(RgwBucketService).fetchAndTransformBuckets(),
        totalNumObjectsSubject.asObservable(),
        totalUsedCapacitySubject.asObservable(),
        averageObjectSizeSubject.asObservable(),
        TestBed.inject(RgwBucketService).getTotalBucketsAndUsersLength()
      ]).subscribe(([daemonData, _, objectCount, usedCapacity, averageSize, bucketData]) => {
        component.rgwDaemonCount = daemonData.length;
        component.objectCount = objectCount;
        component.totalPoolUsedBytes = usedCapacity;
        component.averageObjectSize = averageSize;
        component.rgwBucketCount = bucketData.buckets_count;
        component.UserCount = bucketData.users_count;
      });
    });
    tick();
    expect(listDaemonsSpy).toHaveBeenCalled();
    expect(fetchAndTransformBucketsSpy).toHaveBeenCalled();
    expect(totalBucketsAndUsersSpy).toHaveBeenCalled();
    expect(component.rgwDaemonCount).toEqual(1);
    expect(component.objectCount).toEqual(290);
    expect(component.totalPoolUsedBytes).toEqual(9338880);
    expect(component.averageObjectSize).toEqual(1280);
    expect(component.rgwBucketCount).toEqual(bucketsCount);
    expect(component.UserCount).toEqual(usersCount);
  }));
});
