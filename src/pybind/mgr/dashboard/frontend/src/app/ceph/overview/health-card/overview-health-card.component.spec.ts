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
      memory: { total: 8, ok: 8, warn: 0, critical: 0 },
      storage: { total: 4, ok: 2, warn: 1, critical: 1 },
      processors: { total: 2, ok: 2, warn: 0, critical: 0 },
      network: { total: 6, ok: 4, warn: 1, critical: 1 },
      power: { total: 4, ok: 4, warn: 0, critical: 0 },
      fans: { total: 12, ok: 8, warn: 2, critical: 2 },
      temperatures: { total: 3, ok: 3, warn: 0, critical: 0 }
    },
    total: { total: 39, ok: 31, warn: 4, critical: 4 }
  },
  host: { flawed: 2 }
};

const EXPECTED_ICON_MAP: Record<string, string> = {
  memory: 'dataEnrichment',
  storage: 'vmdkDisk',
  processors: 'chip',
  network: 'network1',
  power: 'plug',
  fans: 'ibmStreamSets',
  temperatures: 'temperature'
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

  describe('hardware data', () => {
    it('should emit 7 hardware rows in 4 sections', (done) => {
      component.hardwareData$.subscribe((hwData) => {
        expect(hwData).not.toBeNull();
        expect(hwData!.sections.length).toBe(4);
        expect(hwData!.sections.flat().length).toBe(7);
        done();
      });
    });

    it('should map each row to correct icon from HARDWARE_ICON_MAP', (done) => {
      component.hardwareData$.subscribe((hwData) => {
        const rows = hwData!.sections.flat();
        rows.forEach((row) => {
          expect(row.icon).toBe(EXPECTED_ICON_MAP[row.key]);
        });
        done();
      });
    });

    it('should map each row to correct label from HardwareNameMapping', (done) => {
      component.hardwareData$.subscribe((hwData) => {
        const rows = hwData!.sections.flat();
        rows.forEach((row) => {
          expect(row.label).toBe(HardwareNameMapping[row.key as keyof typeof HardwareNameMapping]);
        });
        done();
      });
    });

    it('should build statusCounts with correct ok/warn/critical counts', (done) => {
      component.hardwareData$.subscribe((hwData) => {
        const rows = hwData!.sections.flat();

        const memoryRow = rows.find((r) => r.key === 'memory');
        expect(memoryRow?.statusCounts).toEqual([{ icon: 'success', count: 8 }]);

        const storageRow = rows.find((r) => r.key === 'storage');
        expect(storageRow?.statusCounts).toEqual([
          { icon: 'error', count: 1 },
          { icon: 'warningAltFilled', count: 1 },
          { icon: 'success', count: 2 }
        ]);

        const fansRow = rows.find((r) => r.key === 'fans');
        expect(fansRow?.statusCounts).toEqual([
          { icon: 'error', count: 2 },
          { icon: 'warningAltFilled', count: 2 },
          { icon: 'success', count: 8 }
        ]);
        done();
      });
    });

    it('should compute correct severity per row', (done) => {
      component.hardwareData$.subscribe((hwData) => {
        const rows = hwData!.sections.flat();

        const memoryRow = rows.find((r) => r.key === 'memory');
        expect(memoryRow?.severity).toBe(0);

        const storageRow = rows.find((r) => r.key === 'storage');
        expect(storageRow?.severity).toBe(2);

        const powerRow = rows.find((r) => r.key === 'power');
        expect(powerRow?.severity).toBe(0);
        done();
      });
    });

    it('should compute overall severity as worst across all categories', (done) => {
      component.hardwareData$.subscribe((hwData) => {
        expect(hwData!.overallSeverity).toBe('error');
        done();
      });
    });

    it('should include all 7 categories', (done) => {
      component.hardwareData$.subscribe((hwData) => {
        const keys = hwData!.sections.flat().map((r) => r.key);
        expect(keys).toEqual([
          'memory',
          'storage',
          'processors',
          'network',
          'power',
          'fans',
          'temperatures'
        ]);
        done();
      });
    });
  });
});

describe('OverviewHealthCardComponent (all healthy)', () => {
  let component: OverviewHealthCardComponent;
  let fixture: ComponentFixture<OverviewHealthCardComponent>;

  const healthyMock = {
    total: {
      category: {
        memory: { total: 4, ok: 4, warn: 0, critical: 0 },
        storage: { total: 2, ok: 2, warn: 0, critical: 0 },
        processors: { total: 1, ok: 1, warn: 0, critical: 0 },
        network: { total: 2, ok: 2, warn: 0, critical: 0 },
        power: { total: 2, ok: 2, warn: 0, critical: 0 },
        fans: { total: 4, ok: 4, warn: 0, critical: 0 },
        temperatures: { total: 3, ok: 3, warn: 0, critical: 0 }
      },
      total: { total: 18, ok: 18, warn: 0, critical: 0 }
    },
    host: { flawed: 0 }
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
        {
          provide: SummaryService,
          useValue: { summaryData$: of({ version: 'ceph version 18.0.0 reef (dev)' }) }
        },
        { provide: UpgradeService, useValue: { listCached: jest.fn(() => of(null)) } },
        {
          provide: AuthStorageService,
          useValue: { getPermissions: jest.fn(() => ({ configOpt: { read: true } })) }
        },
        {
          provide: MgrModuleService,
          useValue: { getConfig: jest.fn(() => of({ hw_monitoring: true })) }
        },
        { provide: HardwareService, useValue: { getSummary: jest.fn(() => of(healthyMock)) } },
        { provide: HealthService, useValue: { getTelemetryStatus: jest.fn(() => of(false)) } },
        provideRouter([])
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(OverviewHealthCardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should report overall severity as success when all healthy', (done) => {
    component.hardwareData$.subscribe((hwData) => {
      expect(hwData!.overallSeverity).toBe('success');
      done();
    });
  });

  it('should have only ok statusCounts per row', (done) => {
    component.hardwareData$.subscribe((hwData) => {
      hwData!.sections.flat().forEach((row) => {
        expect(row.statusCounts.length).toBe(1);
        expect(row.statusCounts[0].icon).toBe('success');
      });
      done();
    });
  });
});

describe('OverviewHealthCardComponent (warn only)', () => {
  let component: OverviewHealthCardComponent;
  let fixture: ComponentFixture<OverviewHealthCardComponent>;

  const warnMock = {
    total: {
      category: {
        memory: { total: 4, ok: 3, warn: 1, critical: 0 },
        storage: { total: 2, ok: 2, warn: 0, critical: 0 },
        processors: { total: 1, ok: 1, warn: 0, critical: 0 },
        network: { total: 2, ok: 2, warn: 0, critical: 0 },
        power: { total: 2, ok: 2, warn: 0, critical: 0 },
        fans: { total: 4, ok: 4, warn: 0, critical: 0 },
        temperatures: { total: 3, ok: 3, warn: 0, critical: 0 }
      },
      total: { total: 18, ok: 17, warn: 1, critical: 0 }
    },
    host: { flawed: 1 }
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
        {
          provide: SummaryService,
          useValue: { summaryData$: of({ version: 'ceph version 18.0.0 reef (dev)' }) }
        },
        { provide: UpgradeService, useValue: { listCached: jest.fn(() => of(null)) } },
        {
          provide: AuthStorageService,
          useValue: { getPermissions: jest.fn(() => ({ configOpt: { read: true } })) }
        },
        {
          provide: MgrModuleService,
          useValue: { getConfig: jest.fn(() => of({ hw_monitoring: true })) }
        },
        { provide: HardwareService, useValue: { getSummary: jest.fn(() => of(warnMock)) } },
        { provide: HealthService, useValue: { getTelemetryStatus: jest.fn(() => of(false)) } },
        provideRouter([])
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(OverviewHealthCardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should report overall severity as warningAltFilled when only warnings', (done) => {
    component.hardwareData$.subscribe((hwData) => {
      expect(hwData!.overallSeverity).toBe('warningAltFilled');
      done();
    });
  });

  it('should include warn statusCount for memory row', (done) => {
    component.hardwareData$.subscribe((hwData) => {
      const memoryRow = hwData!.sections.flat().find((r) => r.key === 'memory');
      expect(memoryRow?.statusCounts).toEqual([
        { icon: 'warningAltFilled', count: 1 },
        { icon: 'success', count: 3 }
      ]);
      done();
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
          useValue: { summaryData$: of({ version: 'ceph version 18.0.0 reef (dev)' }) }
        },
        { provide: UpgradeService, useValue: { listCached: jest.fn(() => of(null)) } },
        {
          provide: AuthStorageService,
          useValue: { getPermissions: jest.fn(() => ({ configOpt: { read: true } })) }
        },
        {
          provide: MgrModuleService,
          useValue: { getConfig: jest.fn(() => of({ hw_monitoring: false })) }
        },
        { provide: HardwareService, useValue: { getSummary: jest.fn(() => of(null)) } },
        { provide: HealthService, useValue: { getTelemetryStatus: jest.fn(() => of(false)) } },
        provideRouter([])
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(OverviewHealthCardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should emit null hardwareData when hw monitoring is disabled', (done) => {
    component.hardwareData$.subscribe((hwData) => {
      expect(hwData).toBeNull();
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
          useValue: { summaryData$: of({ version: 'ceph version 18.0.0 reef (dev)' }) }
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
