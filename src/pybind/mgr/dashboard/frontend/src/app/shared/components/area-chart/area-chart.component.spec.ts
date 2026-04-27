import { ComponentFixture, TestBed } from '@angular/core/testing';
import { CUSTOM_ELEMENTS_SCHEMA, SimpleChange } from '@angular/core';
import { DatePipe } from '@angular/common';
import { ChartsModule } from '@carbon/charts-angular';
import { AreaChartComponent } from './area-chart.component';
import { NumberFormatterService } from '../../services/number-formatter.service';
import { ChartPoint } from '../../models/area-chart-point';

describe('AreaChartComponent', () => {
  let component: AreaChartComponent;
  let fixture: ComponentFixture<AreaChartComponent>;
  let numberFormatterService: NumberFormatterService;
  let datePipe: DatePipe;

  const mockData: ChartPoint[] = [
    {
      timestamp: new Date('2024-01-01T00:00:00Z'),
      values: { read: 1024, write: 2048 }
    },
    {
      timestamp: new Date('2024-01-01T00:01:00Z'),
      values: { read: 1536, write: 4096 }
    }
  ];

  beforeEach(async () => {
    const numberFormatterMock = {
      formatFromTo: jest.fn().mockReturnValue('1.00'),
      bytesPerSecondLabels: [
        'B/s',
        'KiB/s',
        'MiB/s',
        'GiB/s',
        'TiB/s',
        'PiB/s',
        'EiB/s',
        'ZiB/s',
        'YiB/s'
      ],
      bytesLabels: ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'YiB'],
      unitlessLabels: ['', 'k', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y']
    };

    const datePipeMock = {
      transform: jest.fn().mockReturnValue('01 Jan, 00:00:00')
    };

    await TestBed.configureTestingModule({
      imports: [ChartsModule, AreaChartComponent],
      providers: [
        { provide: NumberFormatterService, useValue: numberFormatterMock },
        { provide: DatePipe, useValue: datePipeMock }
      ],
      schemas: [CUSTOM_ELEMENTS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(AreaChartComponent);
    component = fixture.componentInstance;
    numberFormatterService = TestBed.inject(NumberFormatterService);
    datePipe = TestBed.inject(DatePipe);
  });

  it('should mount', () => {
    expect(component).toBeTruthy();
  });

  it('should render chart data and emit formatted values', () => {
    const emitSpy = jest.spyOn(component.currentFormattedValues, 'emit');

    component.chartTitle = 'Test Chart';
    component.dataUnit = 'B/s';
    component.chartKey = 'test-key';
    component.rawData = mockData;

    (numberFormatterService.formatFromTo as jest.Mock).mockReturnValue('2.00 KiB/s');

    fixture.detectChanges();

    // Trigger ngOnChanges manually
    component.ngOnChanges({
      rawData: new SimpleChange(null, mockData, false)
    });

    expect(emitSpy).toHaveBeenCalledTimes(1);
    expect(emitSpy.mock.calls[emitSpy.mock.calls.length - 1][0]).toEqual({
      key: 'test-key',
      values: expect.objectContaining({
        read: expect.any(String),
        write: expect.any(String)
      })
    });
  });

  it('should set correct chartOptions based on max value', () => {
    component.chartTitle = 'Test Chart';
    component.dataUnit = 'B/s';
    component.rawData = mockData;
    component.chartKey = 'test-key';

    (numberFormatterService.formatFromTo as jest.Mock).mockReturnValue('4.00 KiB/s');

    fixture.detectChanges();

    // Trigger ngOnChanges manually
    component.ngOnChanges({
      rawData: new SimpleChange(null, mockData, false)
    });

    const options = component.chartOptions;
    expect(options?.axes?.left?.domain?.[1]).toBeGreaterThan(0);
    expect(options?.tooltip?.enabled).toBe(true);
    expect(options?.axes?.bottom?.scaleType).toBe('time');
  });

  it('should merge custom options with default chart options', () => {
    component.chartTitle = 'Test Chart';
    component.dataUnit = 'B/s';
    component.rawData = mockData;
    component.customOptions = {
      height: '500px',
      animations: true
    };

    (numberFormatterService.formatFromTo as jest.Mock).mockReturnValue('4.00 KiB/s');

    fixture.detectChanges();
    component.ngOnChanges({
      rawData: new SimpleChange(null, mockData, false)
    });

    expect(component.chartOptions?.height).toBe('500px');
    expect(component.chartOptions?.animations).toBe(true);
    expect(component.chartOptions?.tooltip?.enabled).toBe(true);
  });

  it('should format tooltip with custom date format', () => {
    const testDate = new Date('2024-01-01T12:30:45Z');
    const formattedDate = '01 Jan, 12:30:45';
    const defaultHTML = '<div><p class="value">2024-01-01T12:30:45Z</p></div>';

    (datePipe.transform as jest.Mock).mockReturnValue(formattedDate);

    const result = component.formatChartTooltip(defaultHTML, [{ date: testDate }]);

    expect(datePipe.transform).toHaveBeenCalledWith(testDate, 'dd MMM, HH:mm:ss');
    expect(result).toContain(formattedDate);
    expect(result).not.toContain('2024-01-01T12:30:45Z');
  });

  it('should return default HTML if tooltip data is empty', () => {
    const defaultHTML = '<div><p>Default</p></div>';
    const result = component.formatChartTooltip(defaultHTML, []);
    expect(result).toBe(defaultHTML);
  });

  it('should transform raw data to chart data format correctly', () => {
    component.rawData = mockData;

    fixture.detectChanges();
    component.ngOnChanges({
      rawData: new SimpleChange(null, mockData, false)
    });

    expect(component.chartData.length).toBe(4); // 2 timestamps * 2 groups (read, write)
    expect(component.chartData[0]).toEqual({
      group: 'read',
      date: mockData[0].timestamp,
      value: 1024
    });
    expect(component.chartData[1]).toEqual({
      group: 'write',
      date: mockData[0].timestamp,
      value: 2048
    });
  });

  it('should not emit formatted values if rawData is empty', () => {
    const emitSpy = jest.spyOn(component.currentFormattedValues, 'emit');
    component.rawData = [];
    component.chartKey = 'test-key';

    fixture.detectChanges();
    component.ngOnChanges({
      rawData: new SimpleChange(null, [], false)
    });

    expect(emitSpy).not.toHaveBeenCalled();
  });

  it('should not emit formatted values if values have not changed', () => {
    const emitSpy = jest.spyOn(component.currentFormattedValues, 'emit');
    component.dataUnit = 'B/s';
    component.chartKey = 'test-key';
    component.rawData = mockData;

    (numberFormatterService.formatFromTo as jest.Mock).mockReturnValue('2.00 KiB/s');

    fixture.detectChanges();
    component.ngOnChanges({
      rawData: new SimpleChange(null, mockData, false)
    });

    expect(emitSpy).toHaveBeenCalledTimes(1);

    // Update with same values
    const sameData: ChartPoint[] = [
      {
        timestamp: new Date('2024-01-01T00:02:00Z'),
        values: { read: 1024, write: 2048 } // Same values as first entry
      }
    ];
    component.rawData = sameData;

    component.ngOnChanges({
      rawData: new SimpleChange(mockData, sameData, false)
    });

    // Should not emit again since values are the same
    expect(emitSpy).toHaveBeenCalledTimes(2);
  });

  it('should emit formatted values when values change', () => {
    const emitSpy = jest.spyOn(component.currentFormattedValues, 'emit');
    component.dataUnit = 'B/s';
    component.chartKey = 'test-key';
    component.rawData = mockData;

    (numberFormatterService.formatFromTo as jest.Mock).mockReturnValue('2.00 KiB/s');

    fixture.detectChanges();
    component.ngOnChanges({
      rawData: new SimpleChange(null, mockData, false)
    });

    expect(emitSpy).toHaveBeenCalledTimes(1);

    // Update with different values
    const newData: ChartPoint[] = [
      {
        timestamp: new Date('2024-01-01T00:02:00Z'),
        values: { read: 5120, write: 8192 } // Different values
      }
    ];
    component.rawData = newData;

    (numberFormatterService.formatFromTo as jest.Mock).mockReturnValue('5.00 KiB/s');

    component.ngOnChanges({
      rawData: new SimpleChange(mockData, newData, false)
    });

    // Should emit again since values changed
    expect(emitSpy).toHaveBeenCalledTimes(2);
  });

  it('should set chart title in chart options', () => {
    component.chartTitle = 'Test Chart';
    component.dataUnit = 'B/s';
    component.rawData = mockData;

    (numberFormatterService.formatFromTo as jest.Mock).mockReturnValue('4.00 KiB/s');

    fixture.detectChanges();
    component.ngOnChanges({
      rawData: new SimpleChange(null, mockData, false)
    });
  });
});
