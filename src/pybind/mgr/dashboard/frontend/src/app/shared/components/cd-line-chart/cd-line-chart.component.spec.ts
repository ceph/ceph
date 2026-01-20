import { ComponentFixture, TestBed } from '@angular/core/testing';
import { CdLineChartComponent } from './cd-line-chart.component';

describe('CdLineChartComponent', () => {
  let component: CdLineChartComponent;
  let fixture: ComponentFixture<CdLineChartComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [CdLineChartComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(CdLineChartComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
