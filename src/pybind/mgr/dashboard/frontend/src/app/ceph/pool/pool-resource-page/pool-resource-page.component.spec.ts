import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { ActivatedRoute, convertToParamMap } from '@angular/router';
import { By } from '@angular/platform-browser';
import { of, Subject } from 'rxjs';

import { PoolResourcePageComponent } from './pool-resource-page.component';
import { PoolService } from '~/app/shared/api/pool.service';
import { ErasureCodeProfileService } from '~/app/shared/api/erasure-code-profile.service';
import { FormatterService } from '~/app/shared/services/formatter.service';
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

    it('should render the advanced-properties section', fakeAsync(() => {
      mockActivatedRoute.snapshot.data.section = 'advanced-properties';
      fixture.detectChanges();
      paramMapSubject.next(convertToParamMap({ name: 'test-pool' }));
      tick();
      fixture.detectChanges();

      const el = fixture.nativeElement;
      expect(el.querySelector('cd-table-key-value')).toBeTruthy();
      expect(el.querySelector('cd-resource-overview-card')).toBeFalsy();
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
