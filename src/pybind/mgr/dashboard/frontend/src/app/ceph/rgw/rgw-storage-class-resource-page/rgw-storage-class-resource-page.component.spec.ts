import { Subject, of, throwError } from 'rxjs';
import { RgwStorageClassResourcePageComponent } from './rgw-storage-class-resource-page.component';
import { BucketTieringUtils } from '../utils/rgw-bucket-tiering';
import { TIER_TYPE_DISPLAY } from '../models/rgw-storage-class.model';

describe('RgwStorageClassResourcePageComponent', () => {
  let zonegroupService: any;
  let zoneService: any;
  let formatterService: any;
  let routeParamMap: Subject<any>;
  let routeData: Subject<any>;
  let route: any;
  let component: RgwStorageClassResourcePageComponent;

  beforeEach(() => {
    // Reset mocks before each test
    zonegroupService = {
      getAllZonegroupsInfo: jest.fn().mockReturnValue(of({}))
    };
    zoneService = {
      getAllZonesInfo: jest.fn().mockReturnValue(of({ zones: [] }))
    };
    formatterService = {
      formatToBinary: jest.fn((value: number) => `${value}`)
    };

    routeParamMap = new Subject<any>();
    routeData = new Subject<any>();
    route = {
      parent: {
        paramMap: routeParamMap.asObservable()
      },
      data: routeData.asObservable()
    };

    component = new RgwStorageClassResourcePageComponent(
      route,
      zonegroupService,
      zoneService,
      formatterService
    );
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  const pushRouteParams = (params: Record<string, string>) => {
    routeParamMap.next({
      get: (key: string) => params[key] ?? null
    });
  };

  it('should not call the backend if required parameters are missing', () => {
    component.ngOnInit();

    pushRouteParams({
      zonegroup_name: 'zg-1',
      // placement_target is missing
      storage_class: 'sc-1'
    });

    expect(zonegroupService.getAllZonegroupsInfo).not.toHaveBeenCalled();
    expect(component.details).toBeUndefined();
    expect(component.aclLoading).toBe(false);
  });

  it('should load local zone details if the storage class tier type is LOCAL', () => {
    jest.spyOn(BucketTieringUtils, 'filterAndMapTierTargets').mockReturnValue([
      {
        zonegroup_name: 'zg-1',
        placement_target: 'pt-1',
        storage_class: 'sc-local',
        tier_type: TIER_TYPE_DISPLAY.LOCAL
      }
    ]);

    jest.spyOn(BucketTieringUtils, 'getZoneInfoHelper').mockReturnValue({
      zone_name: 'my-local-zone',
      data_pool: 'my-local-pool'
    });

    component.ngOnInit();

    pushRouteParams({
      zonegroup_name: 'zg-1',
      placement_target: 'pt-1',
      storage_class: 'sc-local'
    });

    expect(zonegroupService.getAllZonegroupsInfo).toHaveBeenCalled();
    expect(zoneService.getAllZonesInfo).toHaveBeenCalled();
    expect(component.details?.zone_name).toBe('my-local-zone');
    expect(component.details?.data_pool).toBe('my-local-pool');
  });

  it('should include target_storage_class and location_constraint in cloud-s3 overview fields', () => {
    jest.spyOn(BucketTieringUtils, 'filterAndMapTierTargets').mockReturnValue([
      {
        zonegroup_name: 'zg-1',
        placement_target: 'pt-1',
        storage_class: 'sc-cloud',
        tier_type: TIER_TYPE_DISPLAY.CLOUD_TIER,
        target_path: '/path',
        target_storage_class: '',
        region: 'default',
        endpoint: 'http://s3.example.com',
        location_constraint: 'eu-north-1'
      }
    ]);

    component.ngOnInit();

    pushRouteParams({
      zonegroup_name: 'zg-1',
      placement_target: 'pt-1',
      storage_class: 'sc-cloud'
    });

    const targetStorageClassField = component.overviewFields.find(
      (field) => field.label === 'Target storage class'
    );
    expect(targetStorageClassField).toEqual(
      expect.objectContaining({
        value: '',
        emptyText: '-'
      })
    );

    const locationConstraintField = component.overviewFields.find(
      (field) => field.label === 'Location constraint'
    );
    expect(locationConstraintField).toEqual(
      expect.objectContaining({
        value: 'eu-north-1'
      })
    );
  });

  it('should handle API errors gracefully', () => {
    zonegroupService.getAllZonegroupsInfo.mockReturnValue(throwError(() => new Error('API Error')));

    component.ngOnInit();

    pushRouteParams({
      zonegroup_name: 'zg-1',
      placement_target: 'pt-1',
      storage_class: 'sc-1'
    });

    expect(component.aclLoading).toBe(false);
    expect(component.details).toBeUndefined();
    expect(component.overviewFields).toBeDefined();
    expect(component.aclKeyValueData).toBeDefined();
  });

  it('should unsubscribe from observables on destroy', () => {
    const unsubscribeSpy = jest.spyOn((component as any).sub, 'unsubscribe');
    component.ngOnDestroy();
    expect(unsubscribeSpy).toHaveBeenCalled();
  });
});
