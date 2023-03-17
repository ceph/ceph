import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CssHelper } from '~/app/shared/classes/css-helper';
import { DimlessBinaryPerSecondPipe } from '~/app/shared/pipes/dimless-binary-per-second.pipe';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { DimlessPipe } from '~/app/shared/pipes/dimless.pipe';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { configureTestBed } from '~/testing/unit-test-helper';
import { DashboardAreaChartComponent } from './dashboard-area-chart.component';

describe('DashboardAreaChartComponent', () => {
  let component: DashboardAreaChartComponent;
  let fixture: ComponentFixture<DashboardAreaChartComponent>;

  configureTestBed({
    schemas: [NO_ERRORS_SCHEMA],
    declarations: [DashboardAreaChartComponent],
    providers: [
      CssHelper,
      DimlessBinaryPipe,
      DimlessBinaryPerSecondPipe,
      DimlessPipe,
      FormatterService
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(DashboardAreaChartComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
