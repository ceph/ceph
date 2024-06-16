import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastrModule } from 'ngx-toastr';
import { SimplebarAngularModule } from 'simplebar-angular';
import { of } from 'rxjs';

import { Permission, Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import {
  Features,
  FeatureTogglesMap,
  FeatureTogglesService
} from '~/app/shared/services/feature-toggles.service';
import { PrometheusAlertService } from '~/app/shared/services/prometheus-alert.service';
import { SummaryService } from '~/app/shared/services/summary.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { NavigationComponent } from './navigation.component';
import { NotificationsComponent } from '../notifications/notifications.component';
import { AdministrationComponent } from '../administration/administration.component';
import { IdentityComponent } from '../identity/identity.component';
import { NgbModule } from '@ng-bootstrap/ng-bootstrap';
import { DashboardHelpComponent } from '../dashboard-help/dashboard-help.component';

function everythingPermittedExcept(disabledPermissions: string[] = []): any {
  const permissions: Permissions = new Permissions({});
  Object.keys(permissions).forEach(
    (key) => (permissions[key] = new Permission(disabledPermissions.includes(key) ? [] : ['read']))
  );
  return permissions;
}

function onlyPermitted(enabledPermissions: string[] = []): any {
  const permissions: Permissions = new Permissions({});
  enabledPermissions.forEach((key) => (permissions[key] = new Permission(['read'])));
  return permissions;
}

function everythingEnabledExcept(features: Features[] = []): FeatureTogglesMap {
  const featureTogglesMap: FeatureTogglesMap = new FeatureTogglesMap();
  features.forEach((key) => (featureTogglesMap[key] = false));
  return featureTogglesMap;
}

function onlyEnabled(features: Features[] = []): FeatureTogglesMap {
  const featureTogglesMap: FeatureTogglesMap = new FeatureTogglesMap();
  Object.keys(featureTogglesMap).forEach(
    (key) => (featureTogglesMap[key] = features.includes(<Features>key))
  );
  return featureTogglesMap;
}

describe('NavigationComponent', () => {
  let component: NavigationComponent;
  let fixture: ComponentFixture<NavigationComponent>;

  configureTestBed({
    declarations: [
      NavigationComponent,
      NotificationsComponent,
      AdministrationComponent,
      DashboardHelpComponent,
      IdentityComponent
    ],
    imports: [
      HttpClientTestingModule,
      SharedModule,
      ToastrModule.forRoot(),
      RouterTestingModule,
      SimplebarAngularModule,
      NgbModule
    ],
    providers: [AuthStorageService, SummaryService, FeatureTogglesService, PrometheusAlertService]
  });

  beforeEach(() => {
    spyOn(TestBed.inject(AuthStorageService), 'getPermissions').and.callFake(() =>
      everythingPermittedExcept()
    );

    spyOn(TestBed.inject(FeatureTogglesService), 'get').and.callFake(() =>
      of(everythingEnabledExcept())
    );
    spyOn(TestBed.inject(SummaryService), 'subscribe').and.callFake(() =>
      of({ health: { status: 'HEALTH_OK' } })
    );
    spyOn(TestBed.inject(PrometheusAlertService), 'getAlerts').and.callFake(() => of([]));
    fixture = TestBed.createComponent(NavigationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  afterEach(() => {
    fixture.destroy();
  });

  describe('Test Permissions', () => {
    const testCases: [string[], string[]][] = [
      [
        ['hosts'],
        [
          '.tc_submenuitem_cluster_hosts',
          '.tc_submenuitem_cluster_inventory',
          '.tc_submenuitem_admin_services'
        ]
      ],
      [['monitor'], ['.tc_submenuitem_cluster_monitor']],
      [['osd'], ['.tc_submenuitem_cluster_osds', '.tc_submenuitem_cluster_crush']],
      [
        ['configOpt'],
        [
          '.tc_submenuitem_admin_configuration',
          '.tc_submenuitem_admin_modules',
          '.tc_submenuitem_admin_users',
          '.tc_submenuitem_admin_upgrade'
        ]
      ],
      [['log'], ['.tc_submenuitem_observe_log']],
      [['prometheus'], ['.tc_submenuitem_observe_monitoring']],
      [['pool'], ['.tc_submenuitem_cluster_pool']],
      [['rbdImage'], ['.tc_submenuitem_block_images']],
      [['rbdMirroring'], ['.tc_submenuitem_block_mirroring']],
      [['iscsi'], ['.tc_submenuitem_block_iscsi']],
      [['rbdImage', 'rbdMirroring', 'iscsi'], ['.tc_menuitem_block']],
      [['nfs'], ['.tc_submenuitem_file_nfs']],
      [['cephfs'], ['.tc_submenuitem_file_cephfs']],
      [
        ['rgw'],
        [
          '.tc_menuitem_rgw',
          '.tc_submenuitem_rgw_daemons',
          '.tc_submenuitem_rgw_buckets',
          '.tc_submenuitem_rgw_users'
        ]
      ]
    ];

    for (const [disabledPermissions, selectors] of testCases) {
      it(`When disabled permissions: ${JSON.stringify(
        disabledPermissions
      )} => hidden: "${selectors}"`, () => {
        component.permissions = everythingPermittedExcept(disabledPermissions);
        component.enabledFeature$ = of(everythingEnabledExcept());

        fixture.detectChanges();
        for (const selector of selectors) {
          expect(fixture.debugElement.query(By.css(selector))).toBeFalsy();
        }
      });
    }

    for (const [enabledPermissions, selectors] of testCases) {
      it(`When enabled permissions: ${JSON.stringify(
        enabledPermissions
      )} => visible: "${selectors}"`, () => {
        component.permissions = onlyPermitted(enabledPermissions);
        component.enabledFeature$ = of(everythingEnabledExcept());

        fixture.detectChanges();
        for (const selector of selectors) {
          expect(fixture.debugElement.query(By.css(selector))).toBeTruthy();
        }
      });
    }
  });

  describe('Test FeatureToggles', () => {
    const testCases: [Features[], string[]][] = [
      [['rbd'], ['.tc_submenuitem_block_images']],
      [['mirroring'], ['.tc_submenuitem_block_mirroring']],
      [['iscsi'], ['.tc_submenuitem_block_iscsi']],
      [['rbd', 'mirroring', 'iscsi'], ['.tc_menuitem_block']],
      [['nfs'], ['.tc_submenuitem_file_nfs']],
      [['cephfs'], ['.tc_submenuitem_file_cephfs']],
      [
        ['rgw'],
        [
          '.tc_menuitem_rgw',
          '.tc_submenuitem_rgw_daemons',
          '.tc_submenuitem_rgw_buckets',
          '.tc_submenuitem_rgw_users'
        ]
      ]
    ];

    for (const [disabledFeatures, selectors] of testCases) {
      it(`When disabled features: ${JSON.stringify(
        disabledFeatures
      )} => hidden: "${selectors}"`, () => {
        component.enabledFeature$ = of(everythingEnabledExcept(disabledFeatures));
        component.permissions = everythingPermittedExcept();

        fixture.detectChanges();
        for (const selector of selectors) {
          expect(fixture.debugElement.query(By.css(selector))).toBeFalsy();
        }
      });
    }

    for (const [enabledFeatures, selectors] of testCases) {
      it(`When enabled features: ${JSON.stringify(
        enabledFeatures
      )} => visible: "${selectors}"`, () => {
        component.enabledFeature$ = of(onlyEnabled(enabledFeatures));
        component.permissions = everythingPermittedExcept();

        fixture.detectChanges();
        for (const selector of selectors) {
          expect(fixture.debugElement.query(By.css(selector))).toBeTruthy();
        }
      });
    }
  });
});
