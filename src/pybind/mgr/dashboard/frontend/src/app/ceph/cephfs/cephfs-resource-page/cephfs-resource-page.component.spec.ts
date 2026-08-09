import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute, convertToParamMap } from '@angular/router';
import { NO_ERRORS_SCHEMA, Pipe, PipeTransform } from '@angular/core';
import { BehaviorSubject, of } from 'rxjs';

import { CephfsResourcePageComponent } from './cephfs-resource-page.component';
import { CephfsService } from '~/app/shared/api/cephfs.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { CephfsResourceStateService } from '~/app/shared/services/cephfs-resource-state.service';
import { CdDatePipe } from '~/app/shared/pipes/cd-date.pipe';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { DimlessPipe } from '~/app/shared/pipes/dimless.pipe';
import { ViewCacheStatus } from '~/app/shared/enum/view-cache-status.enum';

// Mock Pipes
@Pipe({ name: 'cdDate', standalone: false })
class MockCdDatePipe implements PipeTransform {
  transform(value: any) {
    return `${value}_date_formatted`;
  }
}

@Pipe({ name: 'dimlessBinary', standalone: false })
class MockDimlessBinaryPipe implements PipeTransform {
  transform(value: any) {
    return `${value}_binary_formatted`;
  }
}

@Pipe({ name: 'dimless', standalone: false })
class MockDimlessPipe implements PipeTransform {
  transform(value: any) {
    return `${value}_dimless_formatted`;
  }
}

describe('CephfsResourcePageComponent', () => {
  let component: CephfsResourcePageComponent;
  let fixture: ComponentFixture<CephfsResourcePageComponent>;

  let mockCephfsService: { getTabs: jest.Mock };
  let mockAuthStorageService: { getPermissions: jest.Mock };
  let filesystemSubject: BehaviorSubject<any>;

  beforeEach(async () => {
    mockCephfsService = {
      getTabs: jest.fn().mockReturnValue(of({}))
    };

    mockAuthStorageService = {
      getPermissions: jest.fn().mockReturnValue({ grafana: { read: true } })
    };

    filesystemSubject = new BehaviorSubject(null);

    await TestBed.configureTestingModule({
      declarations: [
        CephfsResourcePageComponent,
        MockCdDatePipe,
        MockDimlessBinaryPipe,
        MockDimlessPipe
      ],
      providers: [
        { provide: CephfsService, useValue: mockCephfsService },
        { provide: AuthStorageService, useValue: mockAuthStorageService },
        {
          provide: CephfsResourceStateService,
          useValue: { filesystem$: filesystemSubject.asObservable() }
        },
        { provide: CdDatePipe, useClass: MockCdDatePipe },
        { provide: DimlessBinaryPipe, useClass: MockDimlessBinaryPipe },
        { provide: DimlessPipe, useClass: MockDimlessPipe },
        {
          provide: ActivatedRoute,
          useValue: {
            data: of({ section: 'performance' }),
            parent: {
              paramMap: of(convertToParamMap({ id: '42' }))
            }
          }
        }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(CephfsResourcePageComponent);
    component = fixture.componentInstance;
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  describe('ngOnInit (Initialization)', () => {
    it('should set section and id from route parameters', () => {
      fixture.detectChanges();

      expect(component.section).toBe('performance');
      expect(component.id).toBe(42);
    });

    it('should define columns for ranks and pools', () => {
      fixture.detectChanges();

      expect(component.columns.ranks.length).toBe(8);
      expect(component.columns.pools.length).toBe(4);
    });

    it('should sort pools usage correctly via comparator', () => {
      fixture.detectChanges();

      // Find the Usage column
      const usageCol = component.columns.pools.find((col) => col.name === 'Usage');
      const comparator = usageCol?.comparator;
      expect(comparator).toBeDefined();

      if (comparator) {
        // Equal ratio (0.1 vs 0.1)
        expect(comparator(null, null, { used: 10, avail: 100 }, { used: 10, avail: 100 })).toBe(0);

        // A > B (0.2 vs 0.1)
        expect(comparator(null, null, { used: 20, avail: 100 }, { used: 10, avail: 100 })).toBe(1);

        // A < B (0.1 vs 0.5)
        expect(comparator(null, null, { used: 10, avail: 100 }, { used: 50, avail: 100 })).toBe(-1);
      }
    });
  });

  describe('Filesystem State Handling (applyFilesystem)', () => {
    it('should map filesystem properties correctly and trigger tab load', () => {
      const mockFs = {
        id: 42,
        mdsmap: { fs_name: 'test-fs', enabled: true, created: '2026-08-01' },
        cephfs: {}
      };

      const tabsData = {
        standbys: 'daemon1',
        pools: [{ pool: 'data_pool' }],
        ranks: [{ rank: 0 }],
        mds_counters: { mds_0: {} },
        clients: { data: [{ id: 1 }], status: ViewCacheStatus.ValueOk }
      };

      mockCephfsService.getTabs.mockReturnValue(of(tabsData));

      fixture.detectChanges();
      filesystemSubject.next(mockFs);

      expect(component.notFound).toBe(false);
      expect(component.fsName).toBe('test-fs');

      // Check details parsing
      expect(component.details.standbys).toBe('daemon1');
      expect(component.details.pools).toEqual([{ pool: 'data_pool' }]);
      expect(component.details.ranks).toEqual([{ rank: 0 }]);
      expect(component.details.mdsCounters).toEqual({ mds_0: {} });

      // Check overview fields formatting
      expect(component.overviewFields.length).toBe(3);
      expect(component.overviewFields[0].value).toBe('test-fs'); // Name
      expect(component.overviewFields[1].value).toBe('Enabled'); // Status
      expect(component.overviewFields[2].value).toBe('2026-08-01_date_formatted'); // Created
    });

    it('should handle missing mdsmap cleanly', () => {
      const mockFs = {
        id: 42,
        cephfs: { name: 'fallback-fs' }
      };

      fixture.detectChanges();
      filesystemSubject.next(mockFs);

      expect(component.fsName).toBe('fallback-fs');
      expect(component.overviewFields[1].value).toBe('Disabled'); // Status is false/disabled if mdsmap.enabled is not true
    });
  });

  describe('refresh', () => {
    it('should call loadTabs again if ID exists', () => {
      fixture.detectChanges(); // id is set to 42

      mockCephfsService.getTabs.mockClear();
      component.refresh();

      expect(mockCephfsService.getTabs).toHaveBeenCalledWith(42);
    });

    it('should exit early if ID is falsy', () => {
      fixture.detectChanges();
      component.id = 0; // Explicitly break ID

      mockCephfsService.getTabs.mockClear();
      component.refresh();

      expect(mockCephfsService.getTabs).not.toHaveBeenCalled();
    });
  });

  describe('trackByFn', () => {
    it('should return item name', () => {
      const result = component.trackByFn(0, { name: 'test_item' });
      expect(result).toBe('test_item');
    });
  });

  describe('ngOnDestroy', () => {
    it('should unsubscribe from observables', () => {
      fixture.detectChanges();

      const subSpy = jest.spyOn((component as any).sub, 'unsubscribe');
      const tabsSubSpy = jest.spyOn((component as any).tabsSub, 'unsubscribe');

      component.ngOnDestroy();

      expect(subSpy).toHaveBeenCalled();
      expect(tabsSubSpy).toHaveBeenCalled();
    });
  });
});
