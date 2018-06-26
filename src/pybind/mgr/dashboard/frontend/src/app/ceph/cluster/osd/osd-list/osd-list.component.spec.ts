import { HttpClientModule } from '@angular/common/http';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { TabsModule } from 'ngx-bootstrap/tabs';

import { ComponentsModule } from '../../../../shared/components/components.module';
import { DataTableModule } from '../../../../shared/datatable/datatable.module';
import { Permissions } from '../../../../shared/models/permissions';
import { DimlessPipe } from '../../../../shared/pipes/dimless.pipe';
import { AuthStorageService } from '../../../../shared/services/auth-storage.service';
import { FormatterService } from '../../../../shared/services/formatter.service';
import { SharedModule } from '../../../../shared/shared.module';
import { configureTestBed } from '../../../../shared/unit-test-helper';
import { PerformanceCounterModule } from '../../../performance-counter/performance-counter.module';
import { OsdDetailsComponent } from '../osd-details/osd-details.component';
import { OsdPerformanceHistogramComponent } from '../osd-performance-histogram/osd-performance-histogram.component';
import { OsdListComponent } from './osd-list.component';

describe('OsdListComponent', () => {
  let component: OsdListComponent;
  let fixture: ComponentFixture<OsdListComponent>;

  const fakeAuthStorageService = {
    getPermissions: () => {
      return new Permissions({ osd: ['read', 'update', 'create', 'delete'] });
    }
  };

  configureTestBed({
    imports: [
      HttpClientModule,
      PerformanceCounterModule,
      TabsModule.forRoot(),
      DataTableModule,
      ComponentsModule,
      SharedModule
    ],
    declarations: [OsdListComponent, OsdDetailsComponent, OsdPerformanceHistogramComponent],
    providers: [
      DimlessPipe,
      FormatterService,
      { provide: AuthStorageService, useValue: fakeAuthStorageService }
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(OsdListComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
