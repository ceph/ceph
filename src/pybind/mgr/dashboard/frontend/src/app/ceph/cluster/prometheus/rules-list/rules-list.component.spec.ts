import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';

import { configureTestBed } from '../../../../../testing/unit-test-helper';
import { PrometheusService } from '../../../../shared/api/prometheus.service';
import { SettingsService } from '../../../../shared/api/settings.service';
import { SharedModule } from '../../../../shared/shared.module';
import { PrometheusTabsComponent } from '../prometheus-tabs/prometheus-tabs.component';
import { RulesListComponent } from './rules-list.component';

describe('RulesListComponent', () => {
  let component: RulesListComponent;
  let fixture: ComponentFixture<RulesListComponent>;

  configureTestBed({
    declarations: [RulesListComponent, PrometheusTabsComponent],
    imports: [
      HttpClientTestingModule,
      SharedModule,
      NgbNavModule,
      RouterTestingModule,
      ToastrModule.forRoot()
    ],
    providers: [PrometheusService, SettingsService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RulesListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
