import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CssHelper } from '~/app/shared/classes/css-helper';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { configureTestBed } from '~/testing/unit-test-helper';
import { DashboardPieComponent } from './dashboard-pie.component';

describe('DashboardPieComponent', () => {
  let component: DashboardPieComponent;
  let fixture: ComponentFixture<DashboardPieComponent>;

  configureTestBed({
    schemas: [NO_ERRORS_SCHEMA],
    declarations: [DashboardPieComponent],
    providers: [CssHelper, DimlessBinaryPipe]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(DashboardPieComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
