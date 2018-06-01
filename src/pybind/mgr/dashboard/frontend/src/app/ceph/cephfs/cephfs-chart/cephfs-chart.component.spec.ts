import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ChartsModule } from 'ng2-charts/ng2-charts';

import { configureTestBed } from '../../../shared/unit-test-helper';
import { CephfsChartComponent } from './cephfs-chart.component';

describe('CephfsChartComponent', () => {
  let component: CephfsChartComponent;
  let fixture: ComponentFixture<CephfsChartComponent>;

  configureTestBed({
    imports: [ChartsModule],
    declarations: [CephfsChartComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CephfsChartComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
