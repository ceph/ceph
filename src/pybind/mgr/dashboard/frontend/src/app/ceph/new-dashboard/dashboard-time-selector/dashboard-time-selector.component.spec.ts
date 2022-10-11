import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { configureTestBed } from '~/testing/unit-test-helper';
import { DashboardTimeSelectorComponent } from './dashboard-time-selector.component';

describe('DashboardTimeSelectorComponent', () => {
  let component: DashboardTimeSelectorComponent;
  let fixture: ComponentFixture<DashboardTimeSelectorComponent>;

  configureTestBed({
    schemas: [NO_ERRORS_SCHEMA],
    declarations: [DashboardTimeSelectorComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(DashboardTimeSelectorComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
