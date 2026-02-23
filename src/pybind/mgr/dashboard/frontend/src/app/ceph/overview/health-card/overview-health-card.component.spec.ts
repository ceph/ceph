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
import { MgrModuleService } from '~/app/shared/api/mgr-module.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';

describe('OverviewStorageCardComponent (Jest)', () => {
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
    getPermissions: jest.fn(() => ({ configOpt: { read: false } }))
  };

  const mockMgrModuleService = {
    getConfig: jest.fn(() => of({ hw_monitoring: false }))
  };

  const mockHardwareService = {
    getSummary: jest.fn(() => of(null))
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
});
