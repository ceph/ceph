import { HttpClientModule } from '@angular/common/http';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AccordionModule, TabsModule } from 'ngx-bootstrap';

import { DataTableModule } from '../../../../shared/datatable/datatable.module';
import { DimlessPipe } from '../../../../shared/pipes/dimless.pipe';
import { FormatterService } from '../../../../shared/services/formatter.service';
import { PerformanceCounterModule } from '../../../performance-counter/performance-counter.module';
import { OsdDetailsComponent } from '../osd-details/osd-details.component';
import {
  OsdPerformanceHistogramComponent
} from '../osd-performance-histogram/osd-performance-histogram.component';
import { OsdService } from '../osd.service';
import { OsdListComponent } from './osd-list.component';

describe('OsdListComponent', () => {
  let component: OsdListComponent;
  let fixture: ComponentFixture<OsdListComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [
        HttpClientModule,
        AccordionModule,
        PerformanceCounterModule,
        TabsModule,
        DataTableModule
      ],
      declarations: [
        OsdListComponent,
        OsdDetailsComponent,
        OsdPerformanceHistogramComponent
      ],
      providers: [OsdService, DimlessPipe, FormatterService]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(OsdListComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
