import { HttpClientModule } from '@angular/common/http';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TabsModule } from 'ngx-bootstrap';

import { DataTableModule } from '../../../../shared/datatable/datatable.module';
import { CdTableSelection } from '../../../../shared/models/cd-table-selection';
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
        TabsModule.forRoot(),
        PerformanceCounterModule,
        DataTableModule
      ],
      declarations: [
        OsdDetailsComponent,
        OsdPerformanceHistogramComponent
      ],
      providers: [OsdService]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(OsdDetailsComponent);
    component = fixture.componentInstance;

    component.selection = new CdTableSelection();

    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
