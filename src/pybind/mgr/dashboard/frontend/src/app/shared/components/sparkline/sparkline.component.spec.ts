import { NO_ERRORS_SCHEMA, SimpleChange } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { configureTestBed } from '~/testing/unit-test-helper';
import { SparklineComponent } from './sparkline.component';
import { ResizeObserver as ResizeObserverPolyfill } from '@juggle/resize-observer';

describe('SparklineComponent', () => {
  let component: SparklineComponent;
  let fixture: ComponentFixture<SparklineComponent>;

  configureTestBed({
    declarations: [SparklineComponent],
    schemas: [NO_ERRORS_SCHEMA],
    providers: [DimlessBinaryPipe, FormatterService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(SparklineComponent);
    component = fixture.componentInstance;
    if (typeof window !== 'undefined') {
      window.ResizeObserver = window.ResizeObserver || ResizeObserverPolyfill;
    }
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
    expect(component.options.plugins.tooltip.external).toBeDefined();
  });

  it('should update', () => {
    expect(component.datasets[0].data).toEqual([]);
    expect(component.chartData.labels.length).toBe(0);

    component.data = [11, 22, 33];
    component.ngOnChanges({ data: new SimpleChange(null, component.data, false) });

    expect(component.datasets[0].data).toEqual([11, 22, 33]);
    expect(component.chartData.labels.length).toBe(3);
  });

  it('should not transform the label, if not isBinary', () => {
    component.isBinary = false;
    const result = component.options.plugins.tooltip.callbacks.label({ parsed: { y: 1024 } });
    expect(result).toBe(1024);
  });

  it('should transform the label, if isBinary', () => {
    component.isBinary = true;
    const result = component.options.plugins.tooltip.callbacks.label({ parsed: { y: 1024 } });
    expect(result).toBe('1 KiB');
  });
});
