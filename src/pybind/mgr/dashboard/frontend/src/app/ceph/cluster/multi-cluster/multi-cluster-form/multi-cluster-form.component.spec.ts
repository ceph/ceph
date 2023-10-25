import { ComponentFixture, TestBed } from '@angular/core/testing';

import { MultiClusterFormComponent } from './multi-cluster-form.component';

describe('MultiClusterFormComponent', () => {
  let component: MultiClusterFormComponent;
  let fixture: ComponentFixture<MultiClusterFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ MultiClusterFormComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(MultiClusterFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
