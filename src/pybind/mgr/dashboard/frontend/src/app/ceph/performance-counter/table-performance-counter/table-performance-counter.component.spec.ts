import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { PerformanceCounterService } from '../../../shared/api/performance-counter.service';
import { TableComponent } from '../../../shared/datatable/table/table.component';
import { DimlessPipe } from '../../../shared/pipes/dimless.pipe';
import { FormatterService } from '../../../shared/services/formatter.service';
import { configureTestBed } from '../../../shared/unit-test-helper';
import { TablePerformanceCounterComponent } from './table-performance-counter.component';

describe('TablePerformanceCounterComponent', () => {
  let component: TablePerformanceCounterComponent;
  let fixture: ComponentFixture<TablePerformanceCounterComponent>;

  const fakeService = {};

  configureTestBed({
    declarations: [TablePerformanceCounterComponent, TableComponent, DimlessPipe],
    imports: [],
    schemas: [NO_ERRORS_SCHEMA],
    providers: [
      { provide: PerformanceCounterService, useValue: fakeService },
      DimlessPipe,
      FormatterService
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(TablePerformanceCounterComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
