import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ChartsModule } from 'ng2-charts/ng2-charts';

import { CephfsChartComponent } from './cephfs-chart.component';

describe('CephfsChartComponent', () => {
  let component: CephfsChartComponent;
  let fixture: ComponentFixture<CephfsChartComponent>;

  beforeEach(
    async(() => {
      TestBed.configureTestingModule({
        imports: [ChartsModule],
        declarations: [CephfsChartComponent]
      }).compileComponents();
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(CephfsChartComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
