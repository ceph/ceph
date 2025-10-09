import { ComponentFixture, TestBed } from '@angular/core/testing';

import { MultiClusterDetailsComponent } from './multi-cluster-details.component';

describe('MultiClusterDetailsComponent', () => {
  let component: MultiClusterDetailsComponent;
  let fixture: ComponentFixture<MultiClusterDetailsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [MultiClusterDetailsComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(MultiClusterDetailsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
