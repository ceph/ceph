import { Component, Input } from '@angular/core';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { Observable } from 'rxjs/Observable';

import { PerformanceCounterService } from '../../../shared/api/performance-counter.service';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { DimlessPipe } from '../../../shared/pipes/dimless.pipe';
import { FormatterService } from '../../../shared/services/formatter.service';
import { TablePerformanceCounterComponent } from './table-performance-counter.component';

@Component({ selector: 'cd-table', template: '' })
class TableStubComponent {
  @Input() data: any[];
  @Input() columns: CdTableColumn[];
  @Input() autoReload: any = 5000;
}

describe('TablePerformanceCounterComponent', () => {
  let component: TablePerformanceCounterComponent;
  let fixture: ComponentFixture<TablePerformanceCounterComponent>;

  const fakeService = {};

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [TablePerformanceCounterComponent, TableStubComponent, DimlessPipe],
      imports: [],
      providers: [
        { provide: PerformanceCounterService, useValue: fakeService },
        DimlessPipe,
        FormatterService
      ]
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(TablePerformanceCounterComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
