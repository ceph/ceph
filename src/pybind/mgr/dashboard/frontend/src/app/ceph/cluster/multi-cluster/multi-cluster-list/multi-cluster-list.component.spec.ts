import { ComponentFixture, TestBed } from '@angular/core/testing';

import { MultiClusterListComponent } from './multi-cluster-list.component';

describe('MultiClusterListComponent', () => {
  let component: MultiClusterListComponent;
  let fixture: ComponentFixture<MultiClusterListComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ MultiClusterListComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(MultiClusterListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
