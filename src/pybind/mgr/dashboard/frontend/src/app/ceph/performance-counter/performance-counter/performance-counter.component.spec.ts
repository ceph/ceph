import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { SharedModule } from '../../../shared/shared.module';
import { TablePerformanceCounterComponent } from '../table-performance-counter/table-performance-counter.component';
import { PerformanceCounterComponent } from './performance-counter.component';

describe('PerformanceCounterComponent', () => {
  let component: PerformanceCounterComponent;
  let fixture: ComponentFixture<PerformanceCounterComponent>;

  configureTestBed({
    declarations: [PerformanceCounterComponent, TablePerformanceCounterComponent],
    imports: [RouterTestingModule, SharedModule, HttpClientTestingModule, BrowserAnimationsModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(PerformanceCounterComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
