import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { ActivatedRoute, convertToParamMap } from '@angular/router';
import { By } from '@angular/platform-browser';
import { BehaviorSubject, of, Subject } from 'rxjs';

import { PoolResourcePageComponent } from './pool-resource-page.component';
import { PoolService } from '~/app/shared/api/pool.service';
import { ErasureCodeProfileService } from '~/app/shared/api/erasure-code-profile.service';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { PipesModule } from '~/app/shared/pipes/pipes.module';
import { RefreshIntervalService } from '~/app/shared/services/refresh-interval.service';
import { PoolType } from '../pool';

describe('PoolResourcePageComponent', () => {
  let component: PoolResourcePageComponent;
  let fixture: ComponentFixture<PoolResourcePageComponent>;

  let mockPoolService: { get: jest.Mock; getConfiguration: jest.Mock; list: jest.Mock };
  let mockEcpService: { list: jest.Mock };
  let mockFormatterService: { format_number: jest.Mock };
  let paramMapSubject: Subject<any>;
  let mockActivatedRoute: any;

  const mockPoolData = {
    pool: 1,
    pool_name: 'test-pool',
    type: PoolType.REPLICATED,
    size: 3,
    min_size: 2,
    crush_rule: 'replicated_rule',
    pg_status: { 'active+clean': 32 },
    application_metadata: ['rbd'],
    quota_max_bytes: 1024000,
    tiers: [2],
    stats: {
      bytes_used: { latest: 500, rate: 0, rates: [] },
      avail_raw: { latest: 1500, rate: 0, rates: [] },
      rd_bytes: { latest: 100, rate: 50, rates: [[1620000000, 50]] },
      rd: { latest: 10, rate: 5, rates: [[1620000000, 50]] },
      wr: { latest: 200, rate: 10, rates: [10] }
    }
  };

  const mockLightweightPoolList = [
    { pool: 1, pool_name: 'test-pool' },
    { pool: 2, pool_name: 'cache-pool', cache_mode: 'writeback' }
  ];

  const mockConfigData = [{ name: 'rbd_qos_bps_limit', value: '1000' }];

  const mockEcpData = [
    { name: 'default', k: 2, m: 1, plugin: 'jerasure' },
    { name: 'ec-profile', k: 4, m: 2, plugin: 'isa' }
  ];

  beforeEach(async () => {
    mockPoolService = {
      get: jest.fn().mockReturnValue(of(mockPoolData)),
      getConfiguration: jest.fn().mockReturnValue(of(mockConfigData)),
      list: jest.fn().mockReturnValue(of(mockLightweightPoolList))
    };

    mockEcpService = {
      list: jest.fn().mockReturnValue(of(mockEcpData))
    };

    mockFormatterService = {
      format_number: jest.fn().mockImplementation((val: any) => `${val} formatted`)
    };

    paramMapSubject = new Subject();
    mockActivatedRoute = {
      parent: { paramMap: paramMapSubject.asObservable() },
      snapshot: { data: { section: 'overview' } }
    };

    await TestBed.configureTestingModule({
      declarations: [PoolResourcePageComponent],
      imports: [PipesModule],
      providers: [
        { provide: ActivatedRoute, useValue: mockActivatedRoute },
        { provide: PoolService, useValue: mockPoolService },
        { provide: ErasureCodeProfileService, useValue: mockEcpService },
        { provide: FormatterService, useValue: mockFormatterService }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(PoolResourcePageComponent);
    component = fixture.componentInstance;
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  describe('Data Initialization', () => {
    it('should clear fields and not call APIs if poolName is empty', fakeAsync(() => {
      fixture.detectChanges();
      paramMapSubject.next(convertToParamMap({ name: null }));
      tick();

      expect(component.poolName).toBe('');
      expect(component.poolOverviewFields.length).toBe(0);
      expect(mockPoolService.get).not.toHaveBeenCalled();
    }));

    it('should fetch all required data when poolName is provided', fakeAsync(() => {
      fixture.detectChanges();
      paramMapSubject.next(convertToParamMap({ name: 'test-pool' }));
      tick();

      expect(component.poolName).toBe('test-pool');
      expect(mockPoolService.get).toHaveBeenCalledWith('test-pool', true);
      expect(mockPoolService.getConfiguration).toHaveBeenCalledWith('test-pool');
      expect(mockPoolService.list).toHaveBeenCalledWith([
        'pool',
        'pool_name',
        'cache_mode',
        'cache_min_evict_age',
        'cache_min_flush_age',
        'target_max_bytes',
        'target_max_objects'
      ]);
      expect(mockEcpService.list).toHaveBeenCalled();
    }));
  });

  describe('Data Transformations', () => {
    beforeEach(fakeAsync(() => {
      fixture.detectChanges();
      paramMapSubject.next(convertToParamMap({ name: 'test-pool' }));
      tick();
    }));

    it('should correctly build overview model for a replicated pool', () => {
      expect(component.overviewModel.name).toBe('test-pool');
      expect(component.overviewModel.type).toBe(PoolType.REPLICATED);
      expect(component.overviewModel.dataProtection).toBe('replica: x3');
      expect(component.overviewModel.pgStatus).toBe('32 active+clean');
      expect(component.overviewModel.usageTotal).toBe(2000);
      expect(component.overviewModel.usagePercent).toBe('25%');
      expect(component.overviewModel.usedCapacity).toBe('500 formatted');
      expect(component.overviewModel.replicationSize).toBe('3');
      expect(component.overviewModel.minSize).toBe('2');
      expect(component.overviewModel.isErasure).toBe(false);
    });

    it('should correctly build overview model for an erasure coded pool', fakeAsync(() => {
      const ecPoolData = {
        ...mockPoolData,
        type: PoolType.ERASURE,
        erasure_code_profile: 'ec-profile'
      };
      mockPoolService.get.mockReturnValue(of(ecPoolData));
      paramMapSubject.next(convertToParamMap({ name: 'test-pool' }));
      tick();

      expect(component.overviewModel.typeLabel).toBe('Erasure Coded');
      expect(component.overviewModel.isErasure).toBe(true);
      expect(component.overviewModel.dataProtection).toBe('EC: ec-profile');
      expect(component.overviewModel.erasureK).toBe('4');
      expect(component.overviewModel.erasureM).toBe('2');
      expect(component.overviewModel.erasureTotal).toBe('6');
      expect(component.overviewModel.erasurePlugin).toBe('isa');
    }));

    it('should correctly map cache tiers from the lightweight pool list', () => {
      expect(component.cacheTiers.length).toBe(1);
      expect(component.cacheTiers[0].pool_name).toBe('cache-pool');
      expect(component.cacheTiers[0].cache_mode).toBe('writeback');
    });

    it('should correctly process rate chart data (arrays and numbers)', () => {
      expect(component.overviewModel.readOpsChartData.length).toBe(1);
      expect(component.overviewModel.readOpsChartData[0].values['Read Ops']).toBe(50);

      expect(component.overviewModel.writeOpsChartData.length).toBe(1);
      expect(component.overviewModel.writeOpsChartData[0].values['Write Ops']).toBe(10);
    });

    it('should correctly build migration model when pool is migrating', fakeAsync(() => {
      // Injected stats: migrated = target objects, total = source + target
      const migratingPoolData = {
        ...mockPoolData,
        migration_src: 2,
        migrating_pgs: '3/32',
        stats: {
          ...mockPoolData.stats,
          num_objects: { latest: 100, rate: 0, rates: [] },
          num_objects_migrated: { latest: 40, rate: 0, rates: [] },
          num_bytes: { latest: 10000, rate: 0, rates: [] },
          num_bytes_migrated: { latest: 4000, rate: 0, rates: [] }
        }
      };
      mockPoolService.get.mockReturnValue(of(migratingPoolData));
      paramMapSubject.next(convertToParamMap({ name: 'test-pool' }));
      tick();

      expect(component.overviewModel.isMigrating).toBe(true);
      expect(component.overviewModel.migrationObjectsMigrated).toBe(40);
      expect(component.overviewModel.migrationObjectsTotal).toBe(100);
      expect(component.overviewModel.migrationBytesMigrated).toBe(4000);
      expect(component.overviewModel.migrationBytesTotal).toBe(10000);
      expect(component.overviewModel.migrationProgress).toBe(40);
      expect(component.overviewModel.migrationMigratingPgs).toBe('3/32');
    }));

    it('should calculate 100% progress when migration is fully completed', fakeAsync(() => {
      const completedMigrationPoolData = {
        ...mockPoolData,
        migration_src: 2,
        stats: {
          ...mockPoolData.stats,
          num_objects: { latest: 100, rate: 0, rates: [] },
          num_objects_migrated: { latest: 100, rate: 0, rates: [] },
          num_bytes: { latest: 10000, rate: 0, rates: [] },
          num_bytes_migrated: { latest: 10000, rate: 0, rates: [] }
        }
      };
      mockPoolService.get.mockReturnValue(of(completedMigrationPoolData));
      paramMapSubject.next(convertToParamMap({ name: 'test-pool' }));
      tick();

      expect(component.overviewModel.isMigrating).toBe(true);
      expect(component.overviewModel.migrationObjectsMigrated).toBe(100);
      expect(component.overviewModel.migrationObjectsTotal).toBe(100);
      expect(component.overviewModel.migrationProgress).toBe(100);
    }));

    it('should resolve source and target names when this pool is the migration target', fakeAsync(() => {
      mockPoolService.get.mockReturnValue(of({ ...mockPoolData, migration_src: 2 }));
      paramMapSubject.next(convertToParamMap({ name: 'test-pool' }));
      tick();

      expect(component.overviewModel.migrationSourceName).toBe('cache-pool');
      expect(component.overviewModel.migrationTargetName).toBe('test-pool');
    }));

    it('should resolve source and target names when this pool is the migration source', fakeAsync(() => {
      mockPoolService.get.mockReturnValue(of({ ...mockPoolData, migration_target: 2 }));
      paramMapSubject.next(convertToParamMap({ name: 'test-pool' }));
      tick();

      expect(component.overviewModel.migrationSourceName).toBe('test-pool');
      expect(component.overviewModel.migrationTargetName).toBe('cache-pool');
    }));

    it('should treat pool id 0 as a migration peer', fakeAsync(() => {
      mockPoolService.get.mockReturnValue(of({ ...mockPoolData, migration_src: 0 }));
      paramMapSubject.next(convertToParamMap({ name: 'test-pool' }));
      tick();

      expect(component.overviewModel.isMigrating).toBe(true);
    }));
  });

  describe('Migration card lifecycle', () => {
    const migratingPool = {
      ...mockPoolData,
      migration_src: 2,
      stats: {
        ...mockPoolData.stats,
        num_objects: { latest: 100, rate: 0, rates: [] },
        num_objects_migrated: { latest: 40, rate: 0, rates: [] },
        num_bytes: { latest: 10000, rate: 0, rates: [] },
        num_bytes_migrated: { latest: 4000, rate: 0, rates: [] }
      }
    };
    let refreshInterval: BehaviorSubject<any>;

    const load = () => {
      mockActivatedRoute.snapshot.data.section = 'overview';
      fixture.detectChanges();
      paramMapSubject.next(convertToParamMap({ name: 'test-pool' }));
      tick();
      fixture.detectChanges();
    };

    beforeEach(() => {
      refreshInterval = new BehaviorSubject<any>(null);
      TestBed.inject(RefreshIntervalService).intervalData$ = refreshInterval.asObservable();
    });

    it('should render the migration card before the pool details card', fakeAsync(() => {
      mockPoolService.get.mockReturnValue(of(migratingPool));
      load();

      const children = Array.from(fixture.nativeElement.children as HTMLCollection).map((el) =>
        el.tagName.toLowerCase()
      );
      expect(children.indexOf('cds-tile')).toBeLessThan(
        children.indexOf('cd-resource-overview-card')
      );
      expect(
        fixture.nativeElement.querySelector('[data-testid="migration-subtitle"]').textContent
      ).toContain("Migrating pool 'test-pool' (source: cache-pool)");
      expect(fixture.nativeElement.querySelector('[data-testid="dismiss-migration"]')).toBeFalsy();
    }));

    it('should keep the card as finished until dismissed once migration ends', fakeAsync(() => {
      mockPoolService.get.mockReturnValue(of(migratingPool));
      load();

      mockPoolService.get.mockReturnValue(of(mockPoolData));
      refreshInterval.next(5000);
      tick();
      fixture.detectChanges();

      expect(component.overviewModel.isMigrating).toBe(false);
      expect(component.migrationFinished).toBe(true);
      expect(component.overviewModel.migrationProgress).toBe(100);
      expect(component.overviewModel.migrationObjectsMigrated).toBe(100);
      expect(component.overviewModel.migrationBytesMigrated).toBe(10000);
      expect(
        fixture.nativeElement.querySelector('[data-testid="migration-subtitle"]').textContent
      ).toContain("Migration finished for pool 'test-pool' (source: cache-pool)");

      fixture.nativeElement.querySelector('[data-testid="dismiss-migration"]').click();
      fixture.detectChanges();

      expect(component.migrationFinished).toBe(false);
      expect(fixture.nativeElement.querySelector('cds-tile')).toBeFalsy();
    }));

    it('should not show a finished card for a pool that was never migrating', fakeAsync(() => {
      load();
      refreshInterval.next(5000);
      tick();

      expect(component.migrationFinished).toBe(false);
    }));

    it('should reset the finished state when navigating to another pool', fakeAsync(() => {
      mockPoolService.get.mockReturnValue(of(migratingPool));
      load();
      mockPoolService.get.mockReturnValue(of(mockPoolData));
      refreshInterval.next(5000);
      tick();
      expect(component.migrationFinished).toBe(true);

      paramMapSubject.next(convertToParamMap({ name: 'other-pool' }));
      tick();

      expect(component.migrationFinished).toBe(false);
    }));
  });

  describe('Template Section Rendering', () => {
    it('should render the overview section when section is "overview"', fakeAsync(() => {
      mockActivatedRoute.snapshot.data.section = 'overview';
      fixture.detectChanges();
      paramMapSubject.next(convertToParamMap({ name: 'test-pool' }));
      tick();
      fixture.detectChanges();

      const el = fixture.nativeElement;
      expect(el.querySelector('cd-resource-overview-card')).toBeTruthy();
      expect(el.querySelector('cd-pool-capacity-protection-card')).toBeTruthy();
      expect(el.querySelector('cd-pool-io-card')).toBeTruthy();
      expect(el.querySelector('cd-table-key-value')).toBeFalsy();
    }));

    it('should render the performance section with Grafana', fakeAsync(() => {
      mockActivatedRoute.snapshot.data.section = 'performance';
      fixture.detectChanges();
      paramMapSubject.next(convertToParamMap({ name: 'test-pool' }));
      tick();
      fixture.detectChanges();

      const grafanaDebugEl = fixture.debugElement.query(By.css('cd-grafana'));
      expect(grafanaDebugEl).toBeTruthy();
      expect(grafanaDebugEl.properties['grafanaPath']).toBe(
        'ceph-pool-details?var-pool_name=test-pool'
      );
    }));

    it('should render the configuration section', fakeAsync(() => {
      mockActivatedRoute.snapshot.data.section = 'configuration';
      fixture.detectChanges();
      paramMapSubject.next(convertToParamMap({ name: 'test-pool' }));
      tick();
      fixture.detectChanges();

      const el = fixture.nativeElement;
      expect(el.querySelector('cd-rbd-configuration-table')).toBeTruthy();

      const tables = el.querySelectorAll('cd-table');
      expect(tables.length).toBe(1);
    }));

    it('should display an error alert if no poolName is present', fakeAsync(() => {
      fixture.detectChanges();
      paramMapSubject.next(convertToParamMap({ name: null }));
      tick();
      fixture.detectChanges();

      const el = fixture.nativeElement;
      const alertPanel = el.querySelector('cd-alert-panel');
      expect(alertPanel).toBeTruthy();
      expect(alertPanel.textContent).toContain('No pool name found');
    }));
  });
});
