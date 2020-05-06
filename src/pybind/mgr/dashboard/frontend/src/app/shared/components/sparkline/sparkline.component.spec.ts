import { NO_ERRORS_SCHEMA, SimpleChange } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { DimlessBinaryPipe } from '../../pipes/dimless-binary.pipe';
import { FormatterService } from '../../services/formatter.service';
import { SparklineComponent } from './sparkline.component';

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
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
    expect(component.options.tooltips.custom).toBeDefined();
  });

  it('should update', () => {
    expect(component.datasets).toEqual([{ data: [] }]);
    expect(component.labels.length).toBe(0);

    component.data = [11, 22, 33];
    component.ngOnChanges({ data: new SimpleChange(null, component.data, false) });

    expect(component.datasets).toEqual([{ data: [11, 22, 33] }]);
    expect(component.labels.length).toBe(3);
  });

  it('should not transform the label, if not isBinary', () => {
    component.isBinary = false;
    const result = component.options.tooltips.callbacks.label({ yLabel: 1024 });
    expect(result).toBe(1024);
  });

  it('should transform the label, if isBinary', () => {
    component.isBinary = true;
    const result = component.options.tooltips.callbacks.label({ yLabel: 1024 });
    expect(result).toBe('1 KiB');
  });
});
