import { NO_ERRORS_SCHEMA } from '@angular/core';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { Observable } from 'rxjs/Observable';

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

  const fakeService = {
    getHealth: () => {
      return Observable.of({
        health: {},
        df: {
          stats: {}
        },
        pools: []
      });
    }
  };

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      providers: [{ provide: DashboardService, useValue: fakeService }],
      imports: [SharedModule],
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
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(HealthComponent);
    component = fixture.componentInstance;

    component.contentData = {
      health: {}
    };

    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
