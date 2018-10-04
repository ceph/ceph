import { HttpClientModule } from '@angular/common/http';

import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AlertModule } from 'ngx-bootstrap';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { SettingsService } from '../../../shared/api/settings.service';
import { InfoPanelComponent } from '../info-panel/info-panel.component';
import { LoadingPanelComponent } from '../loading-panel/loading-panel.component';
import { GrafanaComponent } from './grafana.component';

describe('GrafanaComponent', () => {
  let component: GrafanaComponent;
  let fixture: ComponentFixture<GrafanaComponent>;

  configureTestBed({
    declarations: [GrafanaComponent, InfoPanelComponent, LoadingPanelComponent],
    imports: [AlertModule.forRoot(), HttpClientModule],
    providers: [SettingsService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(GrafanaComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
