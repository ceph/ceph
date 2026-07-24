import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { PoolOverviewModel } from '~/app/shared/models/pool-overview.model';
import { ChartPoint } from '~/app/shared/models/area-chart-point';
import { PoolIoCardComponent } from './pool-io-card.component';

describe('PoolIoCardComponent', () => {
  let component: PoolIoCardComponent;
  let fixture: ComponentFixture<PoolIoCardComponent>;

  const baseOverviewModel: PoolOverviewModel = {
    name: 'test-pool',
    type: 'replicated',
    dataProtection: 'replica: x3',
    applications: ['rbd'],
    pgStatus: 'active+clean',
    crushRuleset: 'replicated_rule',
    usageTotal: 1000,
    usageUsed: 250,
    usagePercent: '25%',
    usedCapacity: '250 B',
    availableCapacity: '750 B',
    totalCapacity: '1000 B',
    quotaLimit: 'No quota',
    isErasure: false,
    typeLabel: 'replicated',
    replicationSize: '3',
    minSize: '2',
    erasureK: '',
    erasureM: '',
    erasureTotal: '',
    erasurePlugin: '',
    failureDomain: 'host',
    readThroughput: '1.5 KiB/s',
    readOps: '15/s',
    readOpsChartData: [],
    writeThroughput: '2.5 KiB/s',
    writeOps: '25/s',
    writeOpsChartData: []
  };

  const dummyChartData: ChartPoint[] = [{ timestamp: new Date(), values: { Ops: 10 } }];

  configureTestBed({
    imports: [SharedModule],
    declarations: [PoolIoCardComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(PoolIoCardComponent);
    component = fixture.componentInstance;
    component.overviewModel = { ...baseOverviewModel };
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  describe('Read Section', () => {
    it('should display read metrics', () => {
      fixture.detectChanges();
      const textContent = fixture.nativeElement.textContent;

      expect(textContent).toContain('Read Throughput');
      expect(textContent).toContain('1.5 KiB/s');
      expect(textContent).toContain('Read Ops');
      expect(textContent).toContain('15/s');
    });

    it('should hide the read chart and show a dash when chart data is empty', () => {
      fixture.detectChanges();
      const chartContainers = fixture.nativeElement.querySelectorAll('.pool-detail-chart');
      expect(chartContainers[0].textContent?.trim()).toBe('-');
    });

    it('should show the read chart when chart data is provided', () => {
      component.overviewModel = {
        ...baseOverviewModel,
        readOpsChartData: dummyChartData
      };
      fixture.detectChanges();

      const areaCharts = fixture.nativeElement.querySelectorAll('cd-area-chart');
      expect(areaCharts.length).toBe(1);
      expect(areaCharts[0].getAttribute('chartTitle')).toBe('Read Ops');
    });
  });

  describe('Write Section', () => {
    it('should display write metrics', () => {
      fixture.detectChanges();
      const textContent = fixture.nativeElement.textContent;

      expect(textContent).toContain('Write Throughput');
      expect(textContent).toContain('2.5 KiB/s');
      expect(textContent).toContain('Write Ops');
      expect(textContent).toContain('25/s');
    });

    it('should hide the write chart and show a dash when chart data is empty', () => {
      fixture.detectChanges();

      const chartContainers = fixture.nativeElement.querySelectorAll('.pool-detail-chart');
      expect(chartContainers[1].textContent?.trim()).toBe('-');
    });

    it('should show the write chart when chart data is provided', () => {
      component.overviewModel = {
        ...baseOverviewModel,
        writeOpsChartData: dummyChartData
      };
      fixture.detectChanges();

      const areaCharts = fixture.nativeElement.querySelectorAll('cd-area-chart');
      expect(areaCharts.length).toBe(1);
      expect(areaCharts[0].getAttribute('chartTitle')).toBe('Write Ops');
    });
  });

  it('should display both charts when both sets of data are provided', () => {
    component.overviewModel = {
      ...baseOverviewModel,
      readOpsChartData: dummyChartData,
      writeOpsChartData: dummyChartData
    };
    fixture.detectChanges();

    const areaCharts = fixture.nativeElement.querySelectorAll('cd-area-chart');
    expect(areaCharts.length).toBe(2);
  });
});
