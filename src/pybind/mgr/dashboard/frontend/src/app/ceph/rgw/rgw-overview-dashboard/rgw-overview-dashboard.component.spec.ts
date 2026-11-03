import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { of, BehaviorSubject, combineLatest, NEVER } from 'rxjs';
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
import { RgwMultisiteService } from '~/app/shared/api/rgw-multisite.service';
import { SharedModule } from '~/app/shared/shared.module';

import { CommonModule } from '@angular/common';
import { ActivatedRoute } from '@angular/router';
import { VERSION_PREFIX } from '~/app/shared/constants/app.constants';

describe('RgwOverviewDashboardComponent', () => {
  let component: RgwOverviewDashboardComponent;
  let fixture: ComponentFixture<RgwOverviewDashboardComponent>;
  let listDaemonsSpy: jest.SpyInstance;
  let listRealmsSpy: jest.SpyInstance;
  let listZonegroupsSpy: jest.SpyInstance;
  let listZonesSpy: jest.SpyInstance;
  let totalBucketsAndUsersSpy: jest.SpyInstance;
  let selectedDaemonSubject: BehaviorSubject<RgwDaemon>;

  const params: Record<string, any> = {};
  const bucketsCount = 2;
  const usersCount = 5;
  const objectsCount = 290;
  const objectsSize = 9338880;
  const daemon: RgwDaemon = {
    id: '8000',
    service_map_id: '4803',
    version: VERSION_PREFIX,
    server_hostname: 'ceph',
    realm_name: 'realm1',
    zonegroup_name: 'zg1-realm1',
    zonegroup_id: 'zg1-id',
    zone_name: 'zone1-zg1-realm1',
    default: true,
    port: 80
  };
  const otherDaemon: RgwDaemon = {
    ...daemon,
    id: '8001',
    service_map_id: '4804',
    default: false
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

  const syncStatus = {
    dataSyncInfo: [{ name: 'zone2' }],
    metadataSyncInfo: {},
    primaryZoneData: ['realm1', 'zg1-realm1', 'zone1-zg1-realm1']
  };

  beforeEach(() => {
    selectedDaemonSubject = new BehaviorSubject<RgwDaemon>(daemon);
    TestBed.configureTestingModule({
      declarations: [
        RgwOverviewDashboardComponent,
        CardComponent,
        CardRowComponent,
        DimlessBinaryPipe
      ],
      schemas: [NO_ERRORS_SCHEMA],
      providers: [
        {
          provide: RgwDaemonService,
          useValue: {
            list: jest.fn(),
            selectedDaemon$: selectedDaemonSubject.asObservable()
          }
        },
        { provide: RgwRealmService, useValue: { list: jest.fn() } },
        { provide: RgwZonegroupService, useValue: { list: jest.fn() } },
        { provide: RgwZoneService, useValue: { list: jest.fn() } },
        {
          provide: RgwBucketService,
          useValue: {
            getTotalBucketsAndUsersLength: jest.fn()
          }
        },
        {
          provide: RgwMultisiteService,
          useValue: {
            getSyncStatus: jest.fn().mockReturnValue(of(syncStatus))
          }
        },
        {
          provide: ActivatedRoute,
          useValue: { params: { subscribe: (fn: Function) => fn(params) } }
        }
      ],
      imports: [HttpClientTestingModule, SharedModule, CommonModule]
    }).compileComponents();
    fixture = TestBed.createComponent(RgwOverviewDashboardComponent);
    component = fixture.componentInstance;
    listDaemonsSpy = jest
      .spyOn(TestBed.inject(RgwDaemonService), 'list')
      .mockReturnValue(of([daemon]));
    totalBucketsAndUsersSpy = jest
      .spyOn(TestBed.inject(RgwBucketService), 'getTotalBucketsAndUsersLength')
      .mockReturnValue(
        of({
          buckets_count: bucketsCount,
          users_count: usersCount,
          objects_count: objectsCount,
          objects_size: objectsSize
        })
      );
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
    const productiveCards =
      fixture.debugElement.nativeElement.querySelectorAll('cd-productive-card');
    expect(productiveCards.length).toBe(3);
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

  it('should transform prometheus data to chart format', () => {
    const mockResults: Record<string, [number, string][]> = {
      RGW_REQUEST_PER_SECOND: [
        [1700000000, '10'],
        [1700000060, '20']
      ],
      AVG_GET_LATENCY: [
        [1700000000, '1.5'],
        [1700000060, '2.0']
      ],
      AVG_PUT_LATENCY: [
        [1700000000, '3.0'],
        [1700000060, '4.0']
      ],
      GET_BANDWIDTH: [
        [1700000000, '1024'],
        [1700000060, '2048']
      ],
      PUT_BANDWIDTH: [
        [1700000000, '512'],
        [1700000060, '768']
      ]
    };

    const perfService = component['performanceCardService'];
    component['getPrometheusData'] = function (_selectedTime: any) {
      this.queriesResults = mockResults;
      this.requestsChartData = perfService.toSeries(
        mockResults.RGW_REQUEST_PER_SECOND,
        'Requests/sec'
      );
      this.latencyChartData = perfService.mergeSeries(
        perfService.toSeries(mockResults.AVG_GET_LATENCY, 'GET'),
        perfService.toSeries(mockResults.AVG_PUT_LATENCY, 'PUT')
      );
      this.bandwidthChartData = perfService.mergeSeries(
        perfService.toSeries(mockResults.GET_BANDWIDTH, 'GET'),
        perfService.toSeries(mockResults.PUT_BANDWIDTH, 'PUT')
      );
    };

    component['getPrometheusData']({});

    expect(component.requestsChartData.length).toBe(2);
    expect(component.requestsChartData[0].values['Requests/sec']).toBe(10);
    expect(component.requestsChartData[0].timestamp).toEqual(new Date(1700000000 * 1000));

    expect(component.latencyChartData.length).toBe(2);
    expect(component.latencyChartData[0].values['GET']).toBe(1.5);
    expect(component.latencyChartData[0].values['PUT']).toBe(3.0);

    expect(component.bandwidthChartData.length).toBe(2);
    expect(component.bandwidthChartData[0].values['GET']).toBe(1024);
    expect(component.bandwidthChartData[0].values['PUT']).toBe(512);
  });

  it('should set component properties from services using combineLatest', fakeAsync(() => {
    component.interval = of(null).subscribe(() => {
      component.fetchDataSub = combineLatest([
        TestBed.inject(RgwDaemonService).list(),
        TestBed.inject(RgwBucketService).getTotalBucketsAndUsersLength()
      ]).subscribe(([daemonData, bucketData]) => {
        component.rgwDaemonCount = daemonData.length;
        component.rgwBucketCount = bucketData.buckets_count;
        component.UserCount = bucketData.users_count;
        component.objectCount = bucketData.objects_count || 0;
        component.totalPoolUsedBytes = bucketData.objects_size || 0;
        component.averageObjectSize =
          component.objectCount > 0 ? component.totalPoolUsedBytes / component.objectCount : 0;
      });
    });
    tick();
    expect(listDaemonsSpy).toHaveBeenCalled();
    expect(totalBucketsAndUsersSpy).toHaveBeenCalled();
    expect(component.rgwDaemonCount).toEqual(1);
    expect(component.objectCount).toEqual(objectsCount);
    expect(component.totalPoolUsedBytes).toEqual(objectsSize);
    expect(component.averageObjectSize).toEqual(objectsSize / objectsCount);
    expect(component.rgwBucketCount).toEqual(bucketsCount);
    expect(component.UserCount).toEqual(usersCount);
  }));

  describe('Object Gateway daemon selection loading', () => {
    it('should not enable loading for the initial daemon selection', () => {
      expect(component.loading).toBe(false);
    });

    it('should not enable loading when the same daemon is re-emitted', () => {
      const getSyncStatusSpy = jest.spyOn(component, 'getSyncStatus');
      getSyncStatusSpy.mockClear();

      selectedDaemonSubject.next({ ...daemon });

      expect(component.loading).toBe(false);
      expect(getSyncStatusSpy).not.toHaveBeenCalled();
    });

    it('should enable loading and refresh sync status when daemon changes', () => {
      const getSyncStatusSpy = jest.spyOn(component, 'getSyncStatus');
      getSyncStatusSpy.mockClear();
      // Keep the request pending so loading is not cleared by the response.
      jest.spyOn(TestBed.inject(RgwMultisiteService), 'getSyncStatus').mockReturnValue(NEVER);

      selectedDaemonSubject.next(otherDaemon);

      expect(component.loading).toBe(true);
      expect(getSyncStatusSpy).toHaveBeenCalledTimes(1);
    });
  });
});
