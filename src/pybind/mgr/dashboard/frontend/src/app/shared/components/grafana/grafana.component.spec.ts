import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { AlertModule } from 'ngx-bootstrap/alert';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { SummaryService } from '../../../shared/services/summary.service';
import { SettingsService } from '../../api/settings.service';
import { CephReleaseNamePipe } from '../../pipes/ceph-release-name.pipe';
import { InfoPanelComponent } from '../info-panel/info-panel.component';
import { LoadingPanelComponent } from '../loading-panel/loading-panel.component';
import { GrafanaComponent } from './grafana.component';

describe('GrafanaComponent', () => {
  let component: GrafanaComponent;
  let fixture: ComponentFixture<GrafanaComponent>;

  configureTestBed({
    declarations: [GrafanaComponent, InfoPanelComponent, LoadingPanelComponent],
    imports: [AlertModule.forRoot(), HttpClientTestingModule, RouterTestingModule],
    providers: [CephReleaseNamePipe, SettingsService, SummaryService, i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(GrafanaComponent);
    component = fixture.componentInstance;
    component.grafanaPath = 'somePath';
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should have found out that grafana does not exist', () => {
    fixture.detectChanges();
    expect(component.grafanaExist).toBe(false);
    expect(component.baseUrl).toBe(undefined);
    expect(component.loading).toBe(true);
    expect(component.url).toBe(undefined);
    expect(component.grafanaSrc).toEqual(undefined);
  });

  describe('with grafana initialized', () => {
    beforeEach(() => {
      TestBed.get(SettingsService).settings = { 'api/grafana/url': 'http:localhost:3000' };
      fixture.detectChanges();
    });

    it('should have found out that grafana exists', () => {
      expect(component.grafanaExist).toBe(true);
      expect(component.baseUrl).toBe('http:localhost:3000/d/');
      expect(component.loading).toBe(false);
      expect(component.url).toBe('http:localhost:3000/d/somePath&refresh=2s&kiosk');
      expect(component.grafanaSrc).toEqual({
        changingThisBreaksApplicationSecurity: 'http:localhost:3000/d/somePath&refresh=2s&kiosk'
      });
    });
  });
});
