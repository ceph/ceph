import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { DimlessBinaryPipe } from '../../../shared/pipes/dimless-binary.pipe';
import { FormatterService } from '../../../shared/services/formatter.service';
import { HealthPieComponent } from './health-pie.component';

describe('HealthPieComponent', () => {
  let component: HealthPieComponent;
  let fixture: ComponentFixture<HealthPieComponent>;

  configureTestBed({
    schemas: [NO_ERRORS_SCHEMA],
    declarations: [HealthPieComponent],
    providers: [DimlessBinaryPipe, FormatterService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(HealthPieComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('Set doughnut if nothing received', () => {
    component.chartType = '';
    fixture.detectChanges();

    expect(component.chartConfig.chartType).toEqual('doughnut');
  });

  it('Set doughnut if not allowed value received', () => {
    component.chartType = 'badType';
    fixture.detectChanges();

    expect(component.chartConfig.chartType).toEqual('doughnut');
  });

  it('Set doughnut if doughnut received', () => {
    component.chartType = 'doughnut';
    fixture.detectChanges();

    expect(component.chartConfig.chartType).toEqual('doughnut');
  });

  it('Set pie if pie received', () => {
    component.chartType = 'pie';
    fixture.detectChanges();

    expect(component.chartConfig.chartType).toEqual('pie');
  });

  it('Add slice border if there is more than one slice with numeric non zero value', () => {
    component.chartConfig.dataset[0].data = [48, 0, 1, 0];
    component.ngOnChanges();

    expect(component.chartConfig.dataset[0].borderWidth).toEqual(1);
  });

  it('Remove slice border if there is only one slice with numeric non zero value', () => {
    component.chartConfig.dataset[0].data = [48, 0, undefined, 0];
    component.ngOnChanges();

    expect(component.chartConfig.dataset[0].borderWidth).toEqual(0);
  });

  it('Remove slice border if there is no slice with numeric non zero value', () => {
    component.chartConfig.dataset[0].data = [undefined, 0];
    component.ngOnChanges();

    expect(component.chartConfig.dataset[0].borderWidth).toEqual(0);
  });

  it('should not hide any slice if there is no user click on legend item', () => {
    const initialData = [8, 15];
    component.chartConfig.dataset[0].data = initialData;
    component.ngOnChanges();

    expect(component.chartConfig.dataset[0].data).toEqual(initialData);
  });
});
