import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { OsdPerformanceHistogramComponent } from './osd-performance-histogram.component';

describe('OsdPerformanceHistogramComponent', () => {
  let component: OsdPerformanceHistogramComponent;
  let fixture: ComponentFixture<OsdPerformanceHistogramComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ OsdPerformanceHistogramComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(OsdPerformanceHistogramComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
