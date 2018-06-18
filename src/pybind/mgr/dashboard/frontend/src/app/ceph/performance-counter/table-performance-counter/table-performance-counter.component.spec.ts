import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SharedModule } from '../../../shared/shared.module';
import { configureTestBed } from '../../../shared/unit-test-helper';
import { TablePerformanceCounterComponent } from './table-performance-counter.component';

describe('TablePerformanceCounterComponent', () => {
  let component: TablePerformanceCounterComponent;
  let fixture: ComponentFixture<TablePerformanceCounterComponent>;

  configureTestBed({
    declarations: [TablePerformanceCounterComponent],
    imports: [SharedModule, HttpClientTestingModule]
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
