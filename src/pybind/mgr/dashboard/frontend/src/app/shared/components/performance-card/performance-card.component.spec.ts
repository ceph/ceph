import { ComponentFixture, TestBed } from '@angular/core/testing';
import { PerformanceCardComponent } from './performance-card.component';

describe('PerformanceCardComponent', () => {
  let component: PerformanceCardComponent;
  let fixture: ComponentFixture<PerformanceCardComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [PerformanceCardComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(PerformanceCardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
