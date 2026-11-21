import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwMultisiteWizardComponent } from './rgw-multisite-wizard.component';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { SharedModule } from '~/app/shared/shared.module';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';
import { CUSTOM_ELEMENTS_SCHEMA, NO_ERRORS_SCHEMA } from '@angular/core';
import { of, Subject } from 'rxjs';

import { RgwDaemonService } from '~/app/shared/api/rgw-daemon.service';
import { RgwMultisiteService } from '~/app/shared/api/rgw-multisite.service';
import { RgwRealmService } from '~/app/shared/api/rgw-realm.service';
import { MultiClusterService } from '~/app/shared/api/multi-cluster.service';
import { MgrModuleService } from '~/app/shared/api/mgr-module.service';
import { SummaryService } from '~/app/shared/services/summary.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import {
  STEP_TITLES_EXISTING_REALM,
  STEP_TITLES_MULTI_CLUSTER_CONFIGURED,
  STEP_TITLES_SINGLE_CLUSTER
} from './multisite-wizard-steps.enum';

/** Minimal MultiClusterConfig response with no remote clusters. */
const SINGLE_CLUSTER_CONFIG = {
  current_url: 'http://localhost',
  current_user: 'admin',
  hub_url: '',
  config: {
    local: [{ url: 'http://localhost', name: 'local', cluster_alias: 'Local', user: 'admin' }]
  }
};

/** MultiClusterConfig response with one remote cluster. */
const MULTI_CLUSTER_CONFIG = {
  current_url: 'http://localhost',
  current_user: 'admin',
  hub_url: '',
  config: {
    local: [{ url: 'http://localhost', name: 'local', cluster_alias: 'Local', user: 'admin' }],
    remote: [{ url: 'http://remote', name: 'remote-fsid', cluster_alias: 'Remote', user: 'admin' }]
  }
};

const EMPTY_REALMS = { default_info: '', realms: [] };
const SOME_REALMS = { default_info: 'realm-a', realms: ['realm-a', 'realm-b'] };

function buildDaemon(id = 'rgw.1', hostname = 'host1', port = 8080) {
  return { id, server_hostname: hostname, port };
}

describe('RgwMultisiteWizardComponent', () => {
  let component: RgwMultisiteWizardComponent;
  let fixture: ComponentFixture<RgwMultisiteWizardComponent>;

  let daemonSpy: jasmine.Spy;
  let daemonGetSpy: jasmine.Spy;
  let multiClusterSpy: jasmine.Spy;
  let realmListSpy: jasmine.Spy;
  let rgwModuleStatusSpy: jasmine.Spy;
  let setupReplicationSpy: jasmine.Spy;
  let summarySubscribeSpy: jasmine.Spy;
  let mgrModuleUpdateCompleted$: Subject<void>;

  beforeEach(async () => {
    mgrModuleUpdateCompleted$ = new Subject<void>();

    daemonSpy = jasmine.createSpy('list').and.returnValue(of([buildDaemon()]));
    daemonGetSpy = jasmine
      .createSpy('get')
      .and.returnValue(of({ rgw_metadata: { 'frontend_config#0': 'port=8080' } }));
    multiClusterSpy = jasmine.createSpy('getCluster').and.returnValue(of(SINGLE_CLUSTER_CONFIG));
    realmListSpy = jasmine.createSpy('list').and.returnValue(of(EMPTY_REALMS));
    rgwModuleStatusSpy = jasmine.createSpy('getRgwModuleStatus').and.returnValue(of(true));
    setupReplicationSpy = jasmine.createSpy('setUpMultisiteReplication').and.returnValue(of([]));
    summarySubscribeSpy = jasmine.createSpy('subscribe').and.returnValue({ unsubscribe: () => {} });

    await TestBed.configureTestingModule({
      declarations: [RgwMultisiteWizardComponent],
      imports: [HttpClientTestingModule, SharedModule, ReactiveFormsModule, RouterTestingModule],
      schemas: [NO_ERRORS_SCHEMA, CUSTOM_ELEMENTS_SCHEMA],
      providers: [
        NgbActiveModal,
        {
          provide: RgwDaemonService,
          useValue: { list: daemonSpy, get: daemonGetSpy }
        },
        {
          provide: MultiClusterService,
          useValue: { getCluster: multiClusterSpy }
        },
        {
          provide: RgwRealmService,
          useValue: { list: realmListSpy }
        },
        {
          provide: RgwMultisiteService,
          useValue: {
            setUpMultisiteReplication: setupReplicationSpy,
            setRestartGatewayMessage: jasmine.createSpy('setRestartGatewayMessage'),
            getRgwModuleStatus: rgwModuleStatusSpy
          }
        },
        {
          provide: MgrModuleService,
          useValue: {
            updateCompleted$: mgrModuleUpdateCompleted$,
            updateModuleState: jasmine.createSpy('updateModuleState'),
            list: jasmine.createSpy('list').and.returnValue(of([]))
          }
        },
        {
          provide: SummaryService,
          useValue: { subscribe: summarySubscribeSpy }
        },
        {
          provide: NotificationService,
          useValue: { show: jasmine.createSpy('show') }
        }
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwMultisiteWizardComponent);
    component = fixture.componentInstance;
    // Stub cdr.detectChanges() before ngOnInit runs. ChangeDetectorRef is a
    // view-level token that cannot be overridden in the root providers array;
    // spying on the instance is the correct approach. This prevents the
    // component's internal cdr.detectChanges() calls from triggering template
    // rendering, which would fail on Carbon custom-element value accessors
    // (NG01203 / DynamicInputComboboxDirective teardown crash).
    spyOn(component['cdr'], 'detectChanges');
    component.ngOnInit();
  });

  // ─── basic ───────────────────────────────────────────────────────────────────

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  // ─── createForm ──────────────────────────────────────────────────────────────

  describe('createForm', () => {
    it('initialises all expected form controls', () => {
      const controls = [
        'realmName',
        'zonegroupName',
        'zonegroup_endpoints',
        'zoneName',
        'zone_endpoints',
        'username',
        'cluster',
        'replicationZoneName',
        'configType',
        'selectedRealm',
        'secondary_archive_zone'
      ];
      controls.forEach((name) => expect(component.multisiteSetupForm.get(name)).not.toBeNull());
    });

    it('sets sensible default values', () => {
      const f = component.multisiteSetupForm;
      expect(f.get('realmName').value).toBe('default_realm');
      expect(f.get('zonegroupName').value).toBe('default_zonegroup');
      expect(f.get('zoneName').value).toBe('default_zone');
      expect(f.get('username').value).toBe('default_system_user');
      expect(f.get('replicationZoneName').value).toBe('new_replicated_zone');
      expect(f.get('configType').value).toBe('newRealm');
      expect(f.get('secondary_archive_zone').value).toBe(false);
    });
  });

  // ─── ngOnInit — single cluster ───────────────────────────────────────────────

  describe('ngOnInit — single cluster', () => {
    it('sets isMultiClusterConfigured to false when no remote clusters', () => {
      expect(component.isMultiClusterConfigured).toBe(false);
    });

    it('uses STEP_TITLES_SINGLE_CLUSTER step titles', () => {
      const labels = component.stepTitles.map((s) => s.label);
      expect(labels).toEqual(STEP_TITLES_SINGLE_CLUSTER);
    });

    it('clears validators on cluster and replicationZoneName controls', () => {
      expect(component.multisiteSetupForm.get('cluster').validator).toBeNull();
      expect(component.multisiteSetupForm.get('replicationZoneName').validator).toBeNull();
    });

    it('does not show configType radio when there are no realms', () => {
      expect(component.showConfigType).toBe(false);
    });
  });

  // ─── ngOnInit — multi cluster ────────────────────────────────────────────────

  describe('ngOnInit — multi cluster', () => {
    beforeEach(() => {
      multiClusterSpy.and.returnValue(of(MULTI_CLUSTER_CONFIG));
      component.ngOnInit();
    });

    it('sets isMultiClusterConfigured to true', () => {
      expect(component.isMultiClusterConfigured).toBe(true);
    });

    it('uses STEP_TITLES_MULTI_CLUSTER_CONFIGURED step titles', () => {
      const labels = component.stepTitles.map((s) => s.label);
      expect(labels).toEqual(STEP_TITLES_MULTI_CLUSTER_CONFIGURED);
    });

    it('pre-selects the first remote cluster', () => {
      expect(component.selectedCluster).toBe('remote-fsid');
    });

    it('populates clusterDetailsArray with remote clusters only', () => {
      expect(component.clusterDetailsArray.length).toBe(1);
      expect(component.clusterDetailsArray[0].name).toBe('remote-fsid');
    });
  });

  // ─── ngOnInit — realm list ────────────────────────────────────────────────────

  describe('ngOnInit — realm list', () => {
    it('leaves showConfigType false when realm list is empty', () => {
      expect(component.showConfigType).toBe(false);
    });

    it('sets showConfigType true and pre-selects first realm when realms exist', () => {
      realmListSpy.and.returnValue(of(SOME_REALMS));
      multiClusterSpy.and.returnValue(of(MULTI_CLUSTER_CONFIG));
      component.ngOnInit();

      expect(component.showConfigType).toBe(true);
      expect(component.realmList).toEqual(['realm-a', 'realm-b']);
      expect(component.multisiteSetupForm.get('selectedRealm').value).toBe('realm-a');
    });
  });

  // ─── selectedCluster sync via valueChanges ───────────────────────────────────

  describe('selectedCluster sync', () => {
    it('updates selectedCluster when cluster form control value changes', () => {
      component.multisiteSetupForm.get('cluster').setValue('new-fsid');
      expect(component.selectedCluster).toBe('new-fsid');
    });
  });

  // ─── RGW endpoint loading ─────────────────────────────────────────────────────

  describe('loadRGWEndpoints', () => {
    it('calls RgwDaemonService.list on init', () => {
      expect(daemonSpy).toHaveBeenCalled();
    });

    it('builds HTTP endpoint strings from daemon stats', () => {
      expect(component.rgwEndpoints.value).toEqual(['http://host1:8080']);
    });

    it('marks endpoint as HTTPS when frontend_config contains ssl_port', () => {
      daemonGetSpy.and.returnValue(of({ rgw_metadata: { 'frontend_config#0': 'ssl_port=8443' } }));
      component['loadRGWEndpoints']();
      expect(component.rgwEndpoints.value).toEqual(['https://host1:8080']);
    });

    it('sets zonegroup_endpoints and zone_endpoints form controls', () => {
      expect(component.multisiteSetupForm.get('zonegroup_endpoints').value).toEqual([
        'http://host1:8080'
      ]);
      expect(component.multisiteSetupForm.get('zone_endpoints').value).toEqual([
        'http://host1:8080'
      ]);
    });

    it('resets endpoint controls to null when an empty array is set', () => {
      component.multisiteSetupForm.get('zonegroup_endpoints').setValue([]);
      expect(component.multisiteSetupForm.get('zonegroup_endpoints').value).toBeNull();
    });
  });

  // ─── step validity ────────────────────────────────────────────────────────────

  describe('step0Valid', () => {
    it('is true when configType is newRealm and required fields are filled', () => {
      component.multisiteSetupForm.patchValue({
        realmName: 'r',
        zonegroupName: 'zg',
        zonegroup_endpoints: ['http://host:8080']
      });
      expect(component.step0Valid).toBe(true);
    });

    it('is false when realmName is empty and configType is newRealm', () => {
      component.multisiteSetupForm.get('realmName').setValue('');
      expect(component.step0Valid).toBe(false);
    });

    it('is true when configType is existingRealm and selectedRealm is set', () => {
      component.multisiteSetupForm.patchValue({
        configType: 'existingRealm',
        selectedRealm: 'realm-a'
      });
      expect(component.step0Valid).toBe(true);
    });

    it('is true when configType is existingRealm and selectedRealm is null (no validator on selectedRealm)', () => {
      component.multisiteSetupForm.patchValue({
        configType: 'existingRealm',
        selectedRealm: null
      });
      // selectedRealm has no Validators.required, so the control is always
      // valid and step0Valid returns true regardless of the value.
      expect(component.step0Valid).toBe(true);
    });
  });

  describe('step1Valid', () => {
    it('is true when configType is newRealm and zone fields are valid', () => {
      component.multisiteSetupForm.patchValue({
        zoneName: 'zone1',
        zone_endpoints: ['http://host:8080'],
        username: 'user1'
      });
      expect(component.step1Valid).toBe(true);
    });

    it('is false when zoneName is empty', () => {
      component.multisiteSetupForm.get('zoneName').setValue('');
      expect(component.step1Valid).toBe(false);
    });

    it('is true when configType is existingRealm and cluster + replicationZoneName valid', () => {
      component.multisiteSetupForm.patchValue({
        configType: 'existingRealm',
        cluster: 'remote-fsid',
        replicationZoneName: 'rep-zone'
      });
      expect(component.step1Valid).toBe(true);
    });
  });

  describe('step2Valid', () => {
    it('is true when cluster and replicationZoneName have values', () => {
      component.multisiteSetupForm.patchValue({
        cluster: 'remote-fsid',
        replicationZoneName: 'rep-zone'
      });
      expect(component.step2Valid).toBe(true);
    });

    it('is false when cluster is null and the validator is active', () => {
      // The single-cluster init path clears cluster validators; restore them
      // here to match the multi-cluster scenario where step2 is reachable.
      const { Validators } = require('@angular/forms');
      component.multisiteSetupForm.get('cluster').setValidators([Validators.required]);
      component.multisiteSetupForm.get('cluster').setValue(null);
      component.multisiteSetupForm.get('cluster').updateValueAndValidity();
      expect(component.step2Valid).toBe(false);
    });
  });

  // ─── markStep*Touched ─────────────────────────────────────────────────────────

  describe('markStep0Touched', () => {
    it('marks realmName, zonegroupName, zonegroup_endpoints for newRealm', () => {
      component.multisiteSetupForm.get('configType').setValue('newRealm');
      component.markStep0Touched();
      ['realmName', 'zonegroupName', 'zonegroup_endpoints'].forEach((n) => {
        expect(component.multisiteSetupForm.get(n).touched).toBe(true);
        expect(component.multisiteSetupForm.get(n).dirty).toBe(true);
      });
    });

    it('marks selectedRealm for existingRealm', () => {
      component.multisiteSetupForm.get('configType').setValue('existingRealm');
      component.markStep0Touched();
      expect(component.multisiteSetupForm.get('selectedRealm').touched).toBe(true);
    });
  });

  describe('markStep1Touched', () => {
    it('marks zoneName, zone_endpoints, username for newRealm', () => {
      component.multisiteSetupForm.get('configType').setValue('newRealm');
      component.markStep1Touched();
      ['zoneName', 'zone_endpoints', 'username'].forEach((n) => {
        expect(component.multisiteSetupForm.get(n).touched).toBe(true);
      });
    });

    it('marks cluster, replicationZoneName for existingRealm', () => {
      component.multisiteSetupForm.get('configType').setValue('existingRealm');
      component.markStep1Touched();
      ['cluster', 'replicationZoneName'].forEach((n) => {
        expect(component.multisiteSetupForm.get(n).touched).toBe(true);
      });
    });
  });

  // ─── showSubmitButtonLabel ────────────────────────────────────────────────────

  describe('showSubmitButtonLabel', () => {
    it('returns Close when setup is completed', () => {
      component.setupCompleted = true;
      expect(component.showSubmitButtonLabel()).toBe('Close');
    });

    it('returns Configure Multi-Site when multi-cluster configured and not skipped', () => {
      component.setupCompleted = false;
      component.isMultiClusterConfigured = true;
      component.stepsToSkip['Select cluster'] = false;
      expect(component.showSubmitButtonLabel()).toBe('Configure Multi-Site');
    });

    it('returns Export Multi-Site token when single cluster', () => {
      component.setupCompleted = false;
      component.isMultiClusterConfigured = false;
      expect(component.showSubmitButtonLabel()).toBe('Export Multi-Site token');
    });

    it('returns Export Multi-Site token when Select cluster is skipped', () => {
      component.setupCompleted = false;
      component.isMultiClusterConfigured = true;
      component.stepsToSkip['Select cluster'] = true;
      expect(component.showSubmitButtonLabel()).toBe('Export Multi-Site token');
    });
  });

  // ─── skipSelectCluster ────────────────────────────────────────────────────────

  describe('skipSelectCluster', () => {
    it('marks Select cluster as skipped', () => {
      component.skipSelectCluster();
      expect(component.stepsToSkip['Select cluster']).toBe(true);
    });

    it('clears validators on cluster and replicationZoneName', () => {
      // Set validators first so we can verify they are cleared
      component.multisiteSetupForm.get('cluster').setValidators(() => ({ required: true }));
      component.skipSelectCluster();
      expect(component.multisiteSetupForm.get('cluster').validator).toBeNull();
      expect(component.multisiteSetupForm.get('replicationZoneName').validator).toBeNull();
    });
  });

  // ─── onStepChanged ────────────────────────────────────────────────────────────

  describe('onStepChanged', () => {
    beforeEach(() => {
      multiClusterSpy.and.returnValue(of(MULTI_CLUSTER_CONFIG));
      component.ngOnInit();
    });

    it('restores validators when navigating back to Select cluster step after skip', () => {
      component.skipSelectCluster();
      const selectClusterIndex = component.stepTitles.findIndex(
        (s) => s.label === 'Select cluster'
      );
      component.onStepChanged({ current: selectClusterIndex });

      expect(component.stepsToSkip['Select cluster']).toBe(false);
      expect(component.multisiteSetupForm.get('cluster').validator).not.toBeNull();
      expect(component.multisiteSetupForm.get('replicationZoneName').validator).not.toBeNull();
    });

    it('does nothing when navigating to a step that is not Select cluster', () => {
      component.skipSelectCluster();
      component.onStepChanged({ current: 0 });
      expect(component.stepsToSkip['Select cluster']).toBe(true);
    });
  });

  // ─── onValidateStep ───────────────────────────────────────────────────────────

  describe('onValidateStep', () => {
    it('calls markStep0Touched for step 0', () => {
      spyOn(component, 'markStep0Touched');
      component.onValidateStep({ step: 0 });
      expect(component.markStep0Touched).toHaveBeenCalled();
    });

    it('calls markStep1Touched for step 1', () => {
      spyOn(component, 'markStep1Touched');
      component.onValidateStep({ step: 1 });
      expect(component.markStep1Touched).toHaveBeenCalled();
    });

    it('calls markStep2Touched for step 2 when it is not the last step', () => {
      spyOn(component, 'markStep2Touched');
      // Simulate 4-step wizard (multi-cluster configured) by setting stepTitles directly
      component.stepTitles = STEP_TITLES_MULTI_CLUSTER_CONFIGURED.map((label) => ({ label }));
      component.onValidateStep({ step: 2 });
      expect(component.markStep2Touched).toHaveBeenCalled();
    });

    it('does not call markStep2Touched when step 2 is the last step', () => {
      spyOn(component, 'markStep2Touched');
      // 3-step wizard (single cluster), step 2 IS the last step
      component.onValidateStep({ step: 2 });
      expect(component.markStep2Touched).not.toHaveBeenCalled();
    });
  });

  // ─── onConfigTypeChange ───────────────────────────────────────────────────────

  describe('onConfigTypeChange', () => {
    it('switches to STEP_TITLES_EXISTING_REALM when existingRealm selected', () => {
      component.onConfigTypeChange({ value: 'existingRealm' });
      expect(component.stepTitles.map((s) => s.label)).toEqual(STEP_TITLES_EXISTING_REALM);
    });

    it('switches to STEP_TITLES_MULTI_CLUSTER_CONFIGURED when newRealm and multi-cluster', () => {
      component.isMultiClusterConfigured = true;
      component.onConfigTypeChange({ value: 'newRealm' });
      expect(component.stepTitles.map((s) => s.label)).toEqual(
        STEP_TITLES_MULTI_CLUSTER_CONFIGURED
      );
    });

    it('switches to STEP_TITLES_SINGLE_CLUSTER when newRealm and single-cluster', () => {
      component.isMultiClusterConfigured = false;
      component.onConfigTypeChange({ value: 'newRealm' });
      expect(component.stepTitles.map((s) => s.label)).toEqual(STEP_TITLES_SINGLE_CLUSTER);
    });

    it('reads configType from form when no event value is passed', () => {
      component.multisiteSetupForm.get('configType').setValue('existingRealm');
      component.onConfigTypeChange();
      expect(component.stepTitles.map((s) => s.label)).toEqual(STEP_TITLES_EXISTING_REALM);
    });
  });

  // ─── onSubmit — single cluster ───────────────────────────────────────────────

  describe('onSubmit — single cluster (export token)', () => {
    beforeEach(() => {
      // Ensure form is valid
      component.multisiteSetupForm.patchValue({
        realmName: 'my-realm',
        zonegroupName: 'my-zg',
        zonegroup_endpoints: ['http://host:8080'],
        zoneName: 'my-zone',
        zone_endpoints: ['http://host:8080'],
        username: 'admin'
      });
      component.isMultiClusterConfigured = false;
    });

    it('calls setUpMultisiteReplication with correct arguments', () => {
      component.onSubmit();
      expect(setupReplicationSpy).toHaveBeenCalledWith(
        'my-realm',
        'my-zg',
        'http://host:8080',
        'my-zone',
        'http://host:8080',
        'admin'
      );
    });

    it('sets setupCompleted to true on success', () => {
      component.onSubmit();
      expect(component.setupCompleted).toBe(true);
    });

    it('sets loading to false on success', () => {
      component.onSubmit();
      expect(component.loading).toBe(false);
    });

    it('closes the tearsheet when setupCompleted is already true', () => {
      component.tearsheet = { closeTearsheet: jest.fn() } as any;
      component.setupCompleted = true;
      component.onSubmit();
      expect(component.tearsheet.closeTearsheet).toHaveBeenCalled();
      expect(setupReplicationSpy).not.toHaveBeenCalled();
    });

    it('does not submit when form is invalid', () => {
      component.multisiteSetupForm.get('realmName').setValue('');
      component.onSubmit();
      expect(setupReplicationSpy).not.toHaveBeenCalled();
    });
  });

  // ─── onSubmit — multi cluster ─────────────────────────────────────────────────

  describe('onSubmit — multi-cluster (configure replication)', () => {
    const remoteCluster = {
      url: 'http://remote',
      name: 'remote-fsid',
      cluster_alias: 'Remote',
      user: 'admin',
      token: '',
      cluster_connection_status: 0,
      ssl_verify: false,
      ssl_certificate: '',
      ttl: 0
    };

    beforeEach(() => {
      component.isMultiClusterConfigured = true;
      component.clusterDetailsArray = [remoteCluster];
      component.stepsToSkip['Select cluster'] = false;
      component.multisiteSetupForm.patchValue({
        realmName: 'r',
        zonegroupName: 'zg',
        zonegroup_endpoints: ['http://host:8080'],
        zoneName: 'z',
        zone_endpoints: ['http://host:8080'],
        username: 'user',
        cluster: 'remote-fsid',
        replicationZoneName: 'rep-zone'
      });
    });

    it('calls setUpMultisiteReplication with cluster and replicationZoneName', () => {
      component.onSubmit();
      expect(setupReplicationSpy).toHaveBeenCalledWith(
        'r',
        'zg',
        'http://host:8080',
        'z',
        'http://host:8080',
        'user',
        'remote-fsid',
        'rep-zone',
        '',
        [remoteCluster],
        ''
      );
    });

    it('sets completionSummary on success', () => {
      component.onSubmit();
      expect(component.completionSummary).toEqual({
        realm: 'r',
        secondaryCluster: 'Remote - remote-fsid',
        secondaryZone: 'rep-zone'
      });
    });

    it('passes archive tier type when secondary_archive_zone is checked', () => {
      component.multisiteSetupForm.get('secondary_archive_zone').setValue(true);
      component.onSubmit();
      expect(setupReplicationSpy.calls.mostRecent().args[8]).toBe('archive');
    });

    it('sets form cdSubmitButton error on failure', () => {
      setupReplicationSpy.and.returnValue(new (require('rxjs').throwError)('error'));
      component.onSubmit();
      expect(component.multisiteSetupForm.errors).toEqual({ cdSubmitButton: true });
    });
  });

  // ─── onSubmit — RGW module disabled ──────────────────────────────────────────

  describe('onSubmit — RGW module disabled', () => {
    let mgrService: MgrModuleService;

    beforeEach(() => {
      mgrService = TestBed.inject(MgrModuleService);
      component.rgwModuleStatus = false;
      component.isMultiClusterConfigured = false;
      component.multisiteSetupForm.patchValue({
        realmName: 'r',
        zonegroupName: 'zg',
        zonegroup_endpoints: ['http://host:8080'],
        zoneName: 'z',
        zone_endpoints: ['http://host:8080'],
        username: 'user'
      });
    });

    it('calls updateModuleState before proceeding', () => {
      component.onSubmit();
      expect(mgrService.updateModuleState).toHaveBeenCalled();
    });

    it('calls setUpMultisiteReplication after updateCompleted$ emits', () => {
      component.onSubmit();
      expect(setupReplicationSpy).not.toHaveBeenCalled();
      mgrModuleUpdateCompleted$.next();
      expect(setupReplicationSpy).toHaveBeenCalled();
    });
  });
});
