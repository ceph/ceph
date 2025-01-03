import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbAlertModule } from '@ng-bootstrap/ng-bootstrap';
import { of } from 'rxjs';

import { SettingsService } from '~/app/shared/api/settings.service';
import { CephReleaseNamePipe } from '~/app/shared/pipes/ceph-release-name.pipe';
import { SummaryService } from '~/app/shared/services/summary.service';
import { configureTestBed } from '~/testing/unit-test-helper';
import { AlertPanelComponent } from '../alert-panel/alert-panel.component';
import { DocComponent } from '../doc/doc.component';
import { LoadingPanelComponent } from '../loading-panel/loading-panel.component';
import { GrafanaComponent } from './grafana.component';

describe('GrafanaComponent', () => {
  let component: GrafanaComponent;
  let fixture: ComponentFixture<GrafanaComponent>;
  const expected_url =
    'http:localhost:3000/d/foo/somePath&refresh=2s&var-datasource=Dashboard1&kiosk&from=now-1h&to=now';
  const expected_logs_url =
    'http:localhost:3000/explore?orgId=1&left={"datasource": "Loki", "queries": [{"refId": "A"}], "range": {"from": "now-1h", "to": "now"}}&kiosk';

  configureTestBed({
    declarations: [GrafanaComponent, AlertPanelComponent, LoadingPanelComponent, DocComponent],
    imports: [NgbAlertModule, HttpClientTestingModule, RouterTestingModule, FormsModule],
    providers: [CephReleaseNamePipe, SettingsService, SummaryService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(GrafanaComponent);
    component = fixture.componentInstance;
    component.grafanaPath = 'somePath';
    component.type = 'metrics';
    component.uid = 'foo';
    component.title = 'panel title';
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
      component.type = 'metrics';
      fixture.detectChanges();
    });

    it('should have found out that grafana exists and dashboard exists', () => {
      expect(component.time).toBe('from=now-1h&to=now');
      expect(component.grafanaExist).toBe(true);
      expect(component.baseUrl).toBe('http:localhost:3000/d/');
      expect(component.loading).toBe(false);
      expect(component.url).toBe(expected_url);
      expect(component.grafanaSrc).toEqual({
        changingThisBreaksApplicationSecurity: expected_url
      });
    });

    it('should reset the values', () => {
      component.reset();
      expect(component.time).toBe('from=now-1h&to=now');
      expect(component.url).toBe(expected_url);
      expect(component.grafanaSrc).toEqual({
        changingThisBreaksApplicationSecurity: expected_url
      });
    });

    it('should have Dashboard', () => {
      TestBed.inject(SettingsService).validateGrafanaDashboardUrl = () => of({ uid: 200 });
      expect(component.dashboardExist).toBe(true);
    });
  });

  describe('with loki datasource', () => {
    beforeEach(() => {
      TestBed.inject(SettingsService)['settings'] = { 'api/grafana/url': 'http:localhost:3000' };
      component.type = 'logs';
      component.grafanaPath = 'explore?';
      fixture.detectChanges();
    });

    it('should have found out that Loki Log Search exists', () => {
      expect(component.grafanaExist).toBe(true);
      expect(component.baseUrl).toBe('http:localhost:3000/d/');
      expect(component.loading).toBe(false);
      expect(component.url).toBe(expected_logs_url);
      expect(component.grafanaSrc).toEqual({
        changingThisBreaksApplicationSecurity: expected_logs_url
      });
    });
  });
});
