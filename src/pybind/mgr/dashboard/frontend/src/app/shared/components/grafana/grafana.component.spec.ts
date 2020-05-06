import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { AlertModule } from 'ngx-bootstrap/alert';
import { of } from 'rxjs';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { SummaryService } from '../../../shared/services/summary.service';
import { SettingsService } from '../../api/settings.service';
import { CephReleaseNamePipe } from '../../pipes/ceph-release-name.pipe';
import { AlertPanelComponent } from '../alert-panel/alert-panel.component';
import { LoadingPanelComponent } from '../loading-panel/loading-panel.component';
import { GrafanaComponent } from './grafana.component';

describe('GrafanaComponent', () => {
  let component: GrafanaComponent;
  let fixture: ComponentFixture<GrafanaComponent>;

  configureTestBed({
    declarations: [GrafanaComponent, AlertPanelComponent, LoadingPanelComponent],
    imports: [AlertModule.forRoot(), HttpClientTestingModule, RouterTestingModule, FormsModule],
    providers: [CephReleaseNamePipe, SettingsService, SummaryService, i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(GrafanaComponent);
    component = fixture.componentInstance;
    component.grafanaPath = 'somePath';
    component.uid = 'foo';
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
      TestBed.inject(SettingsService)['settings'] = { 'api/grafana/url': 'http:localhost:3000' };
      fixture.detectChanges();
    });

    it('should have found out that grafana exists and dashboard exists', () => {
      expect(component.time).toBe('from=now-1h&to=now');
      expect(component.grafanaExist).toBe(true);
      expect(component.baseUrl).toBe('http:localhost:3000/d/');
      expect(component.loading).toBe(false);
      expect(component.url).toBe(
        'http:localhost:3000/d/foo/somePath&refresh=2s&kiosk&from=now-1h&to=now'
      );
      expect(component.grafanaSrc).toEqual({
        changingThisBreaksApplicationSecurity:
          'http:localhost:3000/d/foo/somePath&refresh=2s&kiosk&from=now-1h&to=now'
      });
    });

    it('should reset the values', () => {
      component.reset();
      expect(component.time).toBe('from=now-1h&to=now');
      expect(component.url).toBe(
        'http:localhost:3000/d/foo/somePath&refresh=2s&kiosk&from=now-1h&to=now'
      );
      expect(component.grafanaSrc).toEqual({
        changingThisBreaksApplicationSecurity:
          'http:localhost:3000/d/foo/somePath&refresh=2s&kiosk&from=now-1h&to=now'
      });
    });

    it('should have Dashboard', () => {
      TestBed.inject(SettingsService).validateGrafanaDashboardUrl = () => of({ uid: 200 });
      expect(component.dashboardExist).toBe(true);
    });
  });
});
