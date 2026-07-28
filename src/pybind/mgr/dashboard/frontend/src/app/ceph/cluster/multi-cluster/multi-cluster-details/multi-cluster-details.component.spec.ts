import { ComponentFixture, TestBed } from '@angular/core/testing';

import { MultiClusterDetailsComponent } from './multi-cluster-details.component';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('MultiClusterDetailsComponent', () => {
  let component: MultiClusterDetailsComponent;
  let fixture: ComponentFixture<MultiClusterDetailsComponent>;

  configureTestBed({
    declarations: [MultiClusterDetailsComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(MultiClusterDetailsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
