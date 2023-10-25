import { ComponentFixture, TestBed } from '@angular/core/testing';

import { MultiClusterComponent } from './multi-cluster.component';

describe('MultiClusterComponent', () => {
  let component: MultiClusterComponent;
  let fixture: ComponentFixture<MultiClusterComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ MultiClusterComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(MultiClusterComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
