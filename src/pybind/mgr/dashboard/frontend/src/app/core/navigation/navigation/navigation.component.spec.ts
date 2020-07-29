import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';

import { MockModule } from 'ng-mocks';
import { of } from 'rxjs';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { Permission, Permissions } from '../../../shared/models/permissions';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import {
  Features,
  FeatureTogglesMap,
  FeatureTogglesService
} from '../../../shared/services/feature-toggles.service';
import { PrometheusAlertService } from '../../../shared/services/prometheus-alert.service';
import { SummaryService } from '../../../shared/services/summary.service';
import { NavigationModule } from '../navigation.module';
import { NavigationComponent } from './navigation.component';

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
    declarations: [NavigationComponent],
    imports: [MockModule(NavigationModule)],
    providers: [
      {
        provide: AuthStorageService,
        useValue: {
          getPermissions: jest.fn(),
          isPwdDisplayed$: { subscribe: jest.fn() },
          telemetryNotification$: { subscribe: jest.fn() }
        }
      },
      { provide: SummaryService, useValue: { subscribe: jest.fn() } },
      { provide: FeatureTogglesService, useValue: { get: jest.fn() } },
      { provide: PrometheusAlertService, useValue: { alerts: [] } }
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NavigationComponent);
    component = fixture.componentInstance;
  });

  describe('Test Permissions', () => {
    const testCases: [string[], string[]][] = [
      [
        ['hosts'],
        [
          '.tc_submenuitem_hosts',
          '.tc_submenuitem_cluster_inventory',
          '.tc_submenuitem_cluster_services'
        ]
      ],
      [['monitor'], ['.tc_submenuitem_cluster_monitor']],
      [['osd'], ['.tc_submenuitem_osds', '.tc_submenuitem_crush']],
      [['configOpt'], ['.tc_submenuitem_configuration', '.tc_submenuitem_modules']],
      [['log'], ['.tc_submenuitem_log']],
      [['prometheus'], ['.tc_submenuitem_monitoring']],
      [['pool'], ['.tc_menuitem_pool']],
      [['rbdImage'], ['.tc_submenuitem_block_images']],
      [['rbdMirroring'], ['.tc_submenuitem_block_mirroring']],
      [['iscsi'], ['.tc_submenuitem_block_iscsi']],
      [['rbdImage', 'rbdMirroring', 'iscsi'], ['.tc_menuitem_block']],
      [['nfs'], ['.tc_menuitem_nfs']],
      [['cephfs'], ['.tc_menuitem_cephfs']],
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
      [['nfs'], ['.tc_menuitem_nfs']],
      [['cephfs'], ['.tc_menuitem_cephfs']],
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

  describe('showTopNotification', () => {
    const notification1 = 'notificationName1';
    const notification2 = 'notificationName2';

    beforeEach(() => {
      component.notifications = [];
    });

    it('should show notification', () => {
      component.showTopNotification(notification1, true);
      expect(component.notifications.includes(notification1)).toBeTruthy();
      expect(component.notifications.length).toBe(1);
    });

    it('should not add a second notification if it is already shown', () => {
      component.showTopNotification(notification1, true);
      component.showTopNotification(notification1, true);
      expect(component.notifications.includes(notification1)).toBeTruthy();
      expect(component.notifications.length).toBe(1);
    });

    it('should add a second notification if the first one is different', () => {
      component.showTopNotification(notification1, true);
      component.showTopNotification(notification2, true);
      expect(component.notifications.includes(notification1)).toBeTruthy();
      expect(component.notifications.includes(notification2)).toBeTruthy();
      expect(component.notifications.length).toBe(2);
    });

    it('should hide an active notification', () => {
      component.showTopNotification(notification1, true);
      expect(component.notifications.includes(notification1)).toBeTruthy();
      expect(component.notifications.length).toBe(1);
      component.showTopNotification(notification1, false);
      expect(component.notifications.length).toBe(0);
    });

    it('should not fail if it tries to hide an inactive notification', () => {
      expect(() => component.showTopNotification(notification1, false)).not.toThrow();
      expect(component.notifications.length).toBe(0);
    });

    it('should keep other notifications if it hides one', () => {
      component.showTopNotification(notification1, true);
      component.showTopNotification(notification2, true);
      expect(component.notifications.length).toBe(2);
      component.showTopNotification(notification2, false);
      expect(component.notifications.length).toBe(1);
      expect(component.notifications.includes(notification1)).toBeTruthy();
    });
  });
});
