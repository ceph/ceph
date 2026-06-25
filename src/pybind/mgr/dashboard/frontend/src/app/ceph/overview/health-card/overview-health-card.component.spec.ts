import { ComponentFixture, TestBed } from '@angular/core/testing';
import { of } from 'rxjs';

import { OverviewHealthCardComponent } from './overview-health-card.component';
import { SummaryService } from '~/app/shared/services/summary.service';
import { UpgradeService } from '~/app/shared/api/upgrade.service';
import { provideRouter, RouterModule } from '@angular/router';
import { CommonModule } from '@angular/common';
import { SkeletonModule, ButtonModule, LinkModule } from 'carbon-components-angular';
import { ComponentsModule } from '~/app/shared/components/components.module';
import { ProductiveCardComponent } from '~/app/shared/components/productive-card/productive-card.component';
import { PipesModule } from '~/app/shared/pipes/pipes.module';
import { HardwareService } from '~/app/shared/api/hardware.service';
import { HealthService } from '~/app/shared/api/health.service';
import { MgrModuleService } from '~/app/shared/api/mgr-module.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { HardwareNameMapping } from '~/app/shared/enum/hardware.enum';

const MOCK_HW_SUMMARY = {
  total: {
    category: {
      memory: { total: 8, ok: 8, error: 0 },
      storage: { total: 4, ok: 3, error: 1 },
      processors: { total: 2, ok: 2, error: 0 },
      network: { total: 6, ok: 5, error: 1 },
      power: { total: 4, ok: 4, error: 0 },
      fans: { total: 12, ok: 10, error: 2 }
    },
    total: { total: 36, ok: 32, error: 4 }
  },
  host: { flawed: 2 }
};

const EXPECTED_ICON_MAP: Record<string, string> = {
  memory: 'dataEnrichment',
  storage: 'vmdkDisk',
  processors: 'chip',
  network: 'network1',
  power: 'plug',
  fans: 'ibmStreamSets'
};

describe('OverviewHealthCardComponent', () => {
  let component: OverviewHealthCardComponent;
  let fixture: ComponentFixture<OverviewHealthCardComponent>;

  const summaryServiceMock = {
    summaryData$: of({
      version:
        'ceph version 13.1.0-419-g251e2515b5 (251e2515b563856349498c6caf34e7a282f62937) nautilus (dev)'
    })
  };

  const upgradeServiceMock = {
    listCached: jest.fn(() => of({ versions: [] }))
  };

  const mockAuthStorageService = {
    getPermissions: jest.fn(() => ({ configOpt: { read: true } }))
  };

  const mockMgrModuleService = {
    getConfig: jest.fn(() => of({ hw_monitoring: true }))
  };

  const mockHardwareService = {
    getSummary: jest.fn(() => of(MOCK_HW_SUMMARY))
  };

  const mockHealthService = {
    getTelemetryStatus: jest.fn(() => of(false))
  };

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [
        OverviewHealthCardComponent,
        CommonModule,
        ProductiveCardComponent,
        SkeletonModule,
        ButtonModule,
        RouterModule,
        ComponentsModule,
        LinkModule,
        PipesModule
      ],
      providers: [
        { provide: SummaryService, useValue: summaryServiceMock },
        { provide: UpgradeService, useValue: upgradeServiceMock },
        { provide: AuthStorageService, useValue: mockAuthStorageService },
        { provide: MgrModuleService, useValue: mockMgrModuleService },
        { provide: HardwareService, useValue: mockHardwareService },
        { provide: HealthService, useValue: mockHealthService },
        provideRouter([])
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(OverviewHealthCardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('hardware sections', () => {
    it('should emit hardware rows grouped in pairs', (done) => {
      component.sections$.subscribe((sections) => {
        expect(sections).not.toBeNull();
        sections!.forEach((section) => {
          expect(section.length).toBeLessThanOrEqual(2);
        });
        done();
      });
    });

    it('should limit to 3 groups (6 categories max)', (done) => {
      component.sections$.subscribe((sections) => {
        expect(sections).not.toBeNull();
        expect(sections!.length).toBeLessThanOrEqual(3);
        done();
      });
    });

    it('should map each row to correct icon from HW_ICON_MAP', (done) => {
      component.sections$.subscribe((sections) => {
        const rows = sections!.flat();
        rows.forEach((row) => {
          expect(row.icon).toBe(EXPECTED_ICON_MAP[row.key]);
        });
        done();
      });
    });

    it('should map each row to correct label from HardwareNameMapping', (done) => {
      component.sections$.subscribe((sections) => {
        const rows = sections!.flat();
        rows.forEach((row) => {
          expect(row.label).toBe(
            HardwareNameMapping[row.key as keyof typeof HardwareNameMapping]
          );
        });
        done();
      });
    });

    it('should compute ok and error counts from summary data', (done) => {
      component.sections$.subscribe((sections) => {
        const rows = sections!.flat();
        const memoryRow = rows.find((r) => r.key === 'memory');
        expect(memoryRow?.ok).toBe(8);
        expect(memoryRow?.error).toBe(0);

        const storageRow = rows.find((r) => r.key === 'storage');
        expect(storageRow?.ok).toBe(3);
        expect(storageRow?.error).toBe(1);

        const fansRow = rows.find((r) => r.key === 'fans');
        expect(fansRow?.ok).toBe(10);
        expect(fansRow?.error).toBe(2);
        done();
      });
    });

    it('should include exactly memory, storage, processors, network, power, fans', (done) => {
      component.sections$.subscribe((sections) => {
        const keys = sections!.flat().map((r) => r.key);
        expect(keys).toEqual(['memory', 'storage', 'processors', 'network', 'power', 'fans']);
        done();
      });
    });
  });
});

describe('OverviewHealthCardComponent (hw disabled)', () => {
  let component: OverviewHealthCardComponent;
  let fixture: ComponentFixture<OverviewHealthCardComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [
        OverviewHealthCardComponent,
        CommonModule,
        ProductiveCardComponent,
        SkeletonModule,
        ButtonModule,
        RouterModule,
        ComponentsModule,
        LinkModule,
        PipesModule
      ],
      providers: [
        {
          provide: SummaryService,
          useValue: {
            summaryData$: of({
              version: 'ceph version 18.0.0 reef (dev)'
            })
          }
        },
        { provide: UpgradeService, useValue: { listCached: jest.fn(() => of(null)) } },
        {
          provide: AuthStorageService,
          useValue: { getPermissions: jest.fn(() => ({ configOpt: { read: true } })) }
        },
        { provide: MgrModuleService, useValue: { getConfig: jest.fn(() => of({ hw_monitoring: false })) } },
        { provide: HardwareService, useValue: { getSummary: jest.fn(() => of(null)) } },
        { provide: HealthService, useValue: { getTelemetryStatus: jest.fn(() => of(false)) } },
        provideRouter([])
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(OverviewHealthCardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should emit null sections when hw monitoring is disabled', (done) => {
    component.sections$.subscribe((sections) => {
      expect(sections).toBeNull();
      done();
    });
  });

  it('should report enabled$ as false', (done) => {
    component.enabled$.subscribe((enabled) => {
      expect(enabled).toBe(false);
      done();
    });
  });
});

describe('OverviewHealthCardComponent (no permissions)', () => {
  let component: OverviewHealthCardComponent;
  let fixture: ComponentFixture<OverviewHealthCardComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [
        OverviewHealthCardComponent,
        CommonModule,
        ProductiveCardComponent,
        SkeletonModule,
        ButtonModule,
        RouterModule,
        ComponentsModule,
        LinkModule,
        PipesModule
      ],
      providers: [
        {
          provide: SummaryService,
          useValue: {
            summaryData$: of({
              version: 'ceph version 18.0.0 reef (dev)'
            })
          }
        },
        { provide: UpgradeService, useValue: { listCached: jest.fn(() => of(null)) } },
        {
          provide: AuthStorageService,
          useValue: { getPermissions: jest.fn(() => ({ configOpt: { read: false } })) }
        },
        { provide: MgrModuleService, useValue: { getConfig: jest.fn(() => of({})) } },
        { provide: HardwareService, useValue: { getSummary: jest.fn(() => of(null)) } },
        { provide: HealthService, useValue: { getTelemetryStatus: jest.fn(() => of(false)) } },
        provideRouter([])
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(OverviewHealthCardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should emit false for enabled$ when no configOpt read permission', (done) => {
    component.enabled$.subscribe((enabled) => {
      expect(enabled).toBe(false);
      done();
    });
  });
});
