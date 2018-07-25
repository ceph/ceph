import { HttpClientTestingModule } from '@angular/common/http/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { DashboardService } from '../../../shared/api/dashboard.service';
import { SharedModule } from '../../../shared/shared.module';
import { LogColorPipe } from '../log-color.pipe';
import { MdsSummaryPipe } from '../mds-summary.pipe';
import { MgrSummaryPipe } from '../mgr-summary.pipe';
import { MonSummaryPipe } from '../mon-summary.pipe';
import { OsdSummaryPipe } from '../osd-summary.pipe';
import { PgStatusStylePipe } from '../pg-status-style.pipe';
import { PgStatusPipe } from '../pg-status.pipe';
import { HealthComponent } from './health.component';

describe('HealthComponent', () => {
  let component: HealthComponent;
  let fixture: ComponentFixture<HealthComponent>;

  configureTestBed({
    providers: [DashboardService],
    imports: [SharedModule, HttpClientTestingModule],
    declarations: [
      HealthComponent,
      MonSummaryPipe,
      OsdSummaryPipe,
      MdsSummaryPipe,
      MgrSummaryPipe,
      PgStatusStylePipe,
      LogColorPipe,
      PgStatusPipe
    ],
    schemas: [NO_ERRORS_SCHEMA]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(HealthComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
