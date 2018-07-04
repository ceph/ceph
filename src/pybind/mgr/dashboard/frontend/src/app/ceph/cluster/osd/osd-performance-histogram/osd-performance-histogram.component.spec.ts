import { ComponentFixture, TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../../../testing/unit-test-helper';
import { OsdPerformanceHistogramComponent } from './osd-performance-histogram.component';

describe('OsdPerformanceHistogramComponent', () => {
  let component: OsdPerformanceHistogramComponent;
  let fixture: ComponentFixture<OsdPerformanceHistogramComponent>;

  configureTestBed({
    declarations: [OsdPerformanceHistogramComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(OsdPerformanceHistogramComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
