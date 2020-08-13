import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { DimlessBinaryPipe } from '../../../shared/pipes/dimless-binary.pipe';
import { DimlessPipe } from '../../../shared/pipes/dimless.pipe';
import { FormatterService } from '../../../shared/services/formatter.service';
import { HealthPieComponent } from './health-pie.component';

describe('HealthPieComponent', () => {
  let component: HealthPieComponent;
  let fixture: ComponentFixture<HealthPieComponent>;

  configureTestBed({
    schemas: [NO_ERRORS_SCHEMA],
    declarations: [HealthPieComponent],
    providers: [DimlessBinaryPipe, DimlessPipe, FormatterService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(HealthPieComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
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

  it('should set colors from css variables', () => {
    const cssVar = '--my-color';
    const cssVarColor = '#73c5c5';
    component['getCssVar'] = (name: string) => (name === cssVar ? cssVarColor : '');
    component.chartConfig.colors[0].backgroundColor = [cssVar, '#ffffff'];
    fixture.detectChanges();

    expect(component.chartConfig.colors[0].backgroundColor).toEqual([cssVarColor, '#ffffff']);
  });

  describe('tooltip body', () => {
    const tooltipBody = ['text: 10000'];

    it('should return amount converted to appropriate units', () => {
      component.isBytesData = false;
      expect(component['getChartTooltipBody'](tooltipBody)).toEqual('text: 10 k');

      component.isBytesData = true;
      expect(component['getChartTooltipBody'](tooltipBody)).toEqual('text: 9.8 KiB');
    });

    it('should not return amount when showing label as tooltip', () => {
      component.showLabelAsTooltip = true;
      expect(component['getChartTooltipBody'](tooltipBody)).toEqual('text');
    });
  });
});
