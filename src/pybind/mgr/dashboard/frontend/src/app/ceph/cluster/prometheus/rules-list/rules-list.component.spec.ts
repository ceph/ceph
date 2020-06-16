import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { configureTestBed, i18nProviders } from '../../../../../testing/unit-test-helper';
import { PrometheusService } from '../../../../shared/api/prometheus.service';
import { SettingsService } from '../../../../shared/api/settings.service';
import { SharedModule } from '../../../../shared/shared.module';
import { RulesListComponent } from './rules-list.component';

describe('RulesListComponent', () => {
  let component: RulesListComponent;
  let fixture: ComponentFixture<RulesListComponent>;

  configureTestBed({
    declarations: [RulesListComponent],
    imports: [HttpClientTestingModule, SharedModule],
    providers: [PrometheusService, SettingsService, i18nProviders]
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
