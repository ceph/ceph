import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwConfigDetailsComponent } from './rgw-config-details.component';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('RgwConfigDetailsComponent', () => {
  let component: RgwConfigDetailsComponent;
  let fixture: ComponentFixture<RgwConfigDetailsComponent>;

  configureTestBed({
    declarations: [RgwConfigDetailsComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwConfigDetailsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
