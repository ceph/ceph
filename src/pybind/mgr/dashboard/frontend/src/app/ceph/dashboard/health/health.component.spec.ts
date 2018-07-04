import { HttpClientModule } from '@angular/common/http';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TabsModule } from 'ngx-bootstrap/tabs';

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

  const fakeService = {
    getHealth() {
      return {};
    }
  };

  beforeEach(
    async(() => {
      TestBed.configureTestingModule({
        providers: [{ provide: DashboardService, useValue: fakeService }],
        imports: [SharedModule],
        declarations: [HealthComponent]
      }).compileComponents();
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(HealthComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });
});
