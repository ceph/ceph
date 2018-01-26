import { HttpClientModule } from '@angular/common/http';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AccordionConfig, AccordionModule, TabsModule } from 'ngx-bootstrap';

import { DataTableModule } from '../../../../shared/datatable/datatable.module';
import { PerformanceCounterModule } from '../../../performance-counter/performance-counter.module';
import {
  OsdPerformanceHistogramComponent
} from '../osd-performance-histogram/osd-performance-histogram.component';
import { OsdService } from '../osd.service';
import { OsdDetailsComponent } from './osd-details.component';

describe('OsdDetailsComponent', () => {
  let component: OsdDetailsComponent;
  let fixture: ComponentFixture<OsdDetailsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [
        HttpClientModule,
        AccordionModule,
        TabsModule,
        PerformanceCounterModule,
        DataTableModule
      ],
      declarations: [
        OsdDetailsComponent,
        OsdPerformanceHistogramComponent
      ],
      providers: [OsdService, AccordionConfig]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(OsdDetailsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
