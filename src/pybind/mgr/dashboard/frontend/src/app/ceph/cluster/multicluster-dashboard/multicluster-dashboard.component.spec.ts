import { ComponentFixture, TestBed } from '@angular/core/testing';

import { MulticlusterDashboardComponent } from './multicluster-dashboard.component';

describe('MulticlusterDashboardComponent', () => {
  let component: MulticlusterDashboardComponent;
  let fixture: ComponentFixture<MulticlusterDashboardComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [MulticlusterDashboardComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(MulticlusterDashboardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
