import { NO_ERRORS_SCHEMA, SimpleChange } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CssHelper } from '~/app/shared/classes/css-helper';
import { DimlessBinaryPerSecondPipe } from '~/app/shared/pipes/dimless-binary-per-second.pipe';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { DimlessPipe } from '~/app/shared/pipes/dimless.pipe';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { configureTestBed } from '~/testing/unit-test-helper';
import { DashboardAreaChartComponent } from './dashboard-area-chart.component';

describe('DashboardAreaChartComponent', () => {
  let component: DashboardAreaChartComponent;
  let fixture: ComponentFixture<DashboardAreaChartComponent>;

  configureTestBed({
    schemas: [NO_ERRORS_SCHEMA],
    declarations: [DashboardAreaChartComponent],
    providers: [
      CssHelper,
      DimlessBinaryPipe,
      DimlessBinaryPerSecondPipe,
      DimlessPipe,
      FormatterService
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(DashboardAreaChartComponent);
    component = fixture.componentInstance;
    component.dataArray = [
      [
        [1, '110'],
        [3, '130']
      ],
      [
        [2, '120'],
        [4, '140']
      ],
      [
        [5, '150'],
        [6, '160']
      ]
    ];
    component.labelsArray = ['Read', 'Write', 'Total'];
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should have a chart', () => {
    const chartElement = fixture.debugElement.nativeElement.querySelector('canvas');
    expect(chartElement).toBeTruthy();
  });

  it('should have three datasets', () => {
    component.ngOnChanges({ dataArray: new SimpleChange(null, component.dataArray, false) });
    expect(component.chartData.dataset[0].data).toBeDefined();
    expect(component.chartData.dataset[1].data).toBeDefined();
    expect(component.chartData.dataset[2].data).toBeDefined();
  });

  it('should set label', () => {
    component.ngOnChanges({ dataArray: new SimpleChange(null, component.dataArray, false) });
    expect(component.chartData.dataset[0].label).toEqual('Read');
    expect(component.chartData.dataset[1].label).toEqual('Write');
    expect(component.chartData.dataset[2].label).toEqual('Total');
  });

  it('should transform and update data', () => {
    component.ngOnChanges({ dataArray: new SimpleChange(null, component.dataArray, false) });
    expect(component.chartData.dataset[0].data).toEqual([
      { x: 1000, y: 110 },
      { x: 3000, y: 130 }
    ]);
  });

  it('should set currentData to last value', () => {
    component.ngOnChanges({ dataArray: new SimpleChange(null, component.dataArray, false) });
    expect(component.currentChartData.dataset[0].currentData).toBe('130');
  });

  it('should keep data units consistency', () => {
    // Timeout to be able to access chart object
    setTimeout(() => {
      fixture.detectChanges();

      component.dataUnits = 'B';
      component.ngOnChanges({ dataArray: new SimpleChange(null, component.dataArray, false) });

      expect(component.currentDataUnits).toBe('KiB');
      expect(component.chartDataUnits).toBe('KiB');
    }, 1000);
  });
});
