import { ComponentFixture, TestBed } from '@angular/core/testing';
import { PieChartComponent } from './pie-chart.component';
import { ChartsModule } from '@carbon/charts-angular';
import { CommonModule } from '@angular/common';
import { ChangeDetectionStrategy } from '@angular/core';
import { By } from '@angular/platform-browser';

describe('PieChartComponent', () => {
  let component: PieChartComponent;
  let fixture: ComponentFixture<PieChartComponent>;

  const mockData = [
    { group: 'A', value: 30 },
    { group: 'B', value: 70 }
  ];

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [PieChartComponent, ChartsModule, CommonModule]
    })
      // disable OnPush for test environment
      .overrideComponent(PieChartComponent, {
        set: { changeDetection: ChangeDetectionStrategy.Default }
      })
      .compileComponents();

    fixture = TestBed.createComponent(PieChartComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should prepare chart data on ngOnChanges', () => {
    component.data = mockData;

    component.ngOnChanges({
      data: {
        currentValue: mockData,
        previousValue: null,
        firstChange: true,
        isFirstChange: () => true
      }
    });

    expect(component.chartData.length).toBe(2);
    expect(component.chartData[0]).toEqual({ group: 'A', value: 30 });
    expect(component.chartOptions).toBeDefined();
  });

  it('should set chart options correctly', () => {
    component.data = mockData;
    component.title = 'Test Chart';
    component.height = '300px';
    component.legendPosition = 'top';

    component.ngOnChanges({
      data: {
        currentValue: mockData,
        previousValue: null,
        firstChange: true,
        isFirstChange: () => true
      }
    });

    const opts = component.chartOptions;

    expect(opts.title).toBe('Test Chart');
    expect(opts.height).toBe('300px');
    expect(opts.legend.position).toBe('top');
    expect(opts.pie?.labels?.enabled).toBe(false);
  });

  it('should render ibm-pie-chart when data & options exist', () => {
    component.data = mockData;

    component.ngOnChanges({
      data: {
        currentValue: mockData,
        previousValue: null,
        firstChange: true,
        isFirstChange: () => false
      }
    });

    fixture.detectChanges();

    const chartEl = fixture.debugElement.query(By.css('ibm-pie-chart'));
    expect(chartEl).toBeTruthy();

    expect(chartEl.componentInstance.data).toEqual(component.chartData);
    expect(chartEl.componentInstance.options).toEqual(component.chartOptions);
  });

  it('should NOT render ibm-pie-chart when no data', () => {
    component.data = null as any;

    component.ngOnChanges({
      data: {
        currentValue: null,
        previousValue: mockData,
        firstChange: false,
        isFirstChange: () => false
      }
    });

    fixture.detectChanges();

    const chartEl = fixture.debugElement.query(By.css('ibm-pie-chart'));
    expect(chartEl).toBeNull();
  });
});
