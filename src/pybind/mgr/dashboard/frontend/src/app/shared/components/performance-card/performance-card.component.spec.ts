import { ComponentFixture, TestBed } from '@angular/core/testing';
import { PerformanceCardComponent } from './performance-card.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';

describe('PerformanceCardComponent', () => {
  let component: PerformanceCardComponent;
  let fixture: ComponentFixture<PerformanceCardComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [HttpClientTestingModule, PerformanceCardComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(PerformanceCardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
