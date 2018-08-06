import { SimpleChange } from '@angular/core';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AppModule } from '../../../app.module';
import { DimlessBinaryPipe } from '../../pipes/dimless-binary.pipe';
import { FormatterService } from '../../services/formatter.service';
import { SparklineComponent } from './sparkline.component';

describe('SparklineComponent', () => {
  let component: SparklineComponent;
  let fixture: ComponentFixture<SparklineComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [AppModule],
      providers: [DimlessBinaryPipe, FormatterService]
    }).compileComponents();
  }));

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
    expect(result).toBe('1KiB');
  });
});
