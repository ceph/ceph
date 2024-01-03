import { ComponentFixture, TestBed } from '@angular/core/testing';

import { NgChartsModule } from 'ng2-charts';

import { configureTestBed } from '~/testing/unit-test-helper';
import { CephfsChartComponent } from './cephfs-chart.component';
import { ResizeObserver as ResizeObserverPolyfill } from '@juggle/resize-observer';

describe('CephfsChartComponent', () => {
  let component: CephfsChartComponent;
  let fixture: ComponentFixture<CephfsChartComponent>;

  const counter = [
    [0, 15],
    [5, 15],
    [10, 25],
    [15, 50]
  ];

  configureTestBed({
    imports: [NgChartsModule],
    declarations: [CephfsChartComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CephfsChartComponent);
    component = fixture.componentInstance;
    component.mdsCounter = {
      'mds_server.handle_client_request': counter,
      'mds_mem.ino': counter,
      name: 'a'
    };
    if (typeof window !== 'undefined') {
      window.ResizeObserver = window.ResizeObserver || ResizeObserverPolyfill;
    }
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('completed the chart', () => {
    const lhs = component.chart.datasets[0].data;
    expect(lhs.length).toBe(3);
    expect(lhs).toEqual([
      {
        x: 5000,
        y: 15
      },
      {
        x: 10000,
        y: 25
      },
      {
        x: 15000,
        y: 50
      }
    ]);

    const rhs = component.chart.datasets[1].data;
    expect(rhs.length).toBe(3);
    expect(rhs).toEqual([
      {
        x: 5000,
        y: 0
      },
      {
        x: 10000,
        y: 10
      },
      {
        x: 15000,
        y: 25
      }
    ]);
  });

  it('should force angular to update the chart datasets array in order to update the graph', () => {
    const oldDatasets = component.chart.datasets;
    component.ngOnChanges();
    expect(oldDatasets).toEqual(component.chart.datasets);
    expect(oldDatasets).not.toBe(component.chart.datasets);
  });
});
