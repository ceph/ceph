import { ChangeDetectorRef, Component, NgZone, OnInit, ViewChild } from '@angular/core';
import { Location } from '@angular/common';
import { UntypedFormControl, Validators } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { Observable, forkJoin } from 'rxjs';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { RgwDaemonService } from '~/app/shared/api/rgw-daemon.service';
import { RgwDaemon } from '../models/rgw-daemon';
import { MultiClusterService } from '~/app/shared/api/multi-cluster.service';
import { RgwMultisiteService } from '~/app/shared/api/rgw-multisite.service';
import { ComboBoxItem } from '~/app/shared/models/combo-box.model';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { NotificationService } from '~/app/shared/services/notification.service';
import { map, switchMap } from 'rxjs/operators';
import { Step } from 'carbon-components-angular';
import { TearsheetComponent } from '~/app/shared/components/tearsheet/tearsheet.component';
import { SummaryService } from '~/app/shared/services/summary.service';
import { ExecutingTask } from '~/app/shared/models/executing-task';
import {
  STEP_TITLES_EXISTING_REALM,
  STEP_TITLES_MULTI_CLUSTER_CONFIGURED,
  STEP_TITLES_SINGLE_CLUSTER
} from './multisite-wizard-steps.enum';
import { RgwRealmService } from '~/app/shared/api/rgw-realm.service';
import { MultiCluster, MultiClusterConfig } from '~/app/shared/models/multi-cluster';
import { MgrModuleService } from '~/app/shared/api/mgr-module.service';

interface DaemonStats {
  rgw_metadata?: {
    [key: string]: string;
  };
}

interface RealmsInfo {
  default_info: string;
  realms: string[];
}

interface EndpointInfo {
  hostname: string;
  port: number;
  frontendConfig: string;
}

enum Protocol {
  HTTP = 'http',
  HTTPS = 'https'
}

enum ConfigType {
  NewRealm = 'newRealm',
  ExistingRealm = 'existingRealm'
}

@Component({
  selector: 'cd-rgw-multisite-wizard',
  templateUrl: './rgw-multisite-wizard.component.html',
  styleUrls: ['./rgw-multisite-wizard.component.scss'],
  standalone: false
})
export class RgwMultisiteWizardComponent implements OnInit {
  @ViewChild(TearsheetComponent) tearsheet!: TearsheetComponent;

  multisiteSetupForm!: CdFormGroup;
  permissions!: Permissions;
  stepTitles: Step[] = STEP_TITLES_MULTI_CLUSTER_CONFIGURED.map((title) => ({
    label: title
  }));
  stepsToSkip: { [steps: string]: boolean } = {};
  daemons: RgwDaemon[] = [];
  selectedCluster = '';
  clusterDetailsArray: MultiCluster[] = [];
  isMultiClusterConfigured = false;
  exportTokenForm!: CdFormGroup;
  realms: Array<{ realm: string; token: string }> = [];
  loading = false;
  zonegroupEndpointsOptions: ComboBoxItem[] = [];
  zoneEndpointsOptions: ComboBoxItem[] = [];
  rgwEndpoints: { value: any[] };
  executingTask: ExecutingTask | undefined;
  setupCompleted = false;
  showConfigType = false;
  realmList: string[] = [];
  rgwModuleStatus!: boolean;
  title = $localize`Set up Multi-site Replication`;
  description = $localize`Configure realm, zonegroup, and zone for multi-site replication across clusters.`;
  completionSummary: {
    realm: string;
    secondaryCluster: string;
    secondaryZone: string;
  } | null = null;

  constructor(
    public activeModal: NgbActiveModal,
    private rgwDaemonService: RgwDaemonService,
    private multiClusterService: MultiClusterService,
    private rgwMultisiteService: RgwMultisiteService,
    private rgwRealmService: RgwRealmService,
    public notificationService: NotificationService,
    private summaryService: SummaryService,
    private location: Location,
    private cdr: ChangeDetectorRef,
    private mgrModuleService: MgrModuleService,
    private zone: NgZone
  ) {
    this.createForm();
    this.rgwEndpoints = { value: [] };
  }

  ngOnInit(): void {
    this.loadRGWEndpoints();
    (this.multiClusterService.getCluster() as Observable<MultiClusterConfig>).subscribe(
      (clusters: MultiClusterConfig) => {
        const currentUrl = clusters['current_url'];
        this.clusterDetailsArray = Object.values(clusters['config'])
          .flat()
          .filter((cluster) => cluster['url'] !== currentUrl);
        this.isMultiClusterConfigured = this.clusterDetailsArray.length > 0;
        this.stepTitles = (
          this.isMultiClusterConfigured
            ? STEP_TITLES_MULTI_CLUSTER_CONFIGURED
            : STEP_TITLES_SINGLE_CLUSTER
        ).map((label) => ({ label }));
        this.selectedCluster = this.isMultiClusterConfigured
          ? this.clusterDetailsArray[0]['name']
          : '';

        if (this.isMultiClusterConfigured) {
          this.multisiteSetupForm.get('cluster').setValue(this.clusterDetailsArray[0]['name']);
        }

        if (!this.isMultiClusterConfigured) {
          this.multisiteSetupForm.get('cluster').clearValidators();
          this.multisiteSetupForm.get('cluster').updateValueAndValidity();
          this.multisiteSetupForm.get('replicationZoneName').clearValidators();
          this.multisiteSetupForm.get('replicationZoneName').updateValueAndValidity();
        }
      }
    );

    this.multisiteSetupForm.get('cluster').valueChanges.subscribe((v: string) => {
      this.selectedCluster = v;
    });

    this.summaryService.subscribe((summary) => {
      this.zone.run(() => {
        this.executingTask = summary?.executing_tasks?.find((task) =>
          task.name.includes('progress/Multisite-Setup')
        );
        this.cdr.detectChanges();
      });
    });

    this.stepTitles.forEach((step) => {
      this.stepsToSkip[step.label] = false;
    });

    (this.rgwRealmService.list() as Observable<RealmsInfo>).subscribe((realmsInfo: RealmsInfo) => {
      this.realmList = realmsInfo?.realms || [];
      this.showConfigType = this.realmList.length > 0;
      if (this.showConfigType) {
        this.multisiteSetupForm.get('selectedRealm')?.setValue(this.realmList[0]);
        this.cdr.detectChanges();
      }
    });

    this.rgwMultisiteService.getRgwModuleStatus().subscribe((status: boolean) => {
      this.rgwModuleStatus = status;
    });
  }

  private loadRGWEndpoints(): void {
    this.rgwDaemonService
      .list()
      .pipe(
        switchMap((daemons: RgwDaemon[]) => {
          this.daemons = daemons;
          return this.fetchDaemonStats(daemons);
        })
      )
      .subscribe((daemonStatsArray: EndpointInfo[]) => {
        this.populateRGWEndpoints(daemonStatsArray);
      });
  }

  private fetchDaemonStats(daemons: RgwDaemon[]): Observable<EndpointInfo[]> {
    const observables = daemons.map((daemon) =>
      this.rgwDaemonService.get(daemon.id).pipe(
        map((daemonStats: DaemonStats) => ({
          hostname: daemon.server_hostname,
          port: daemon.port,
          frontendConfig: daemonStats?.rgw_metadata?.['frontend_config#0'] || ''
        }))
      )
    );
    return forkJoin(observables);
  }

  private populateRGWEndpoints(statsArray: EndpointInfo[]): void {
    this.rgwEndpoints.value = statsArray.map((stats: EndpointInfo) => {
      const protocol = stats.frontendConfig.includes('ssl_port') ? Protocol.HTTPS : Protocol.HTTP;
      return `${protocol}://${stats.hostname}:${stats.port}`;
    });
    const makeOptions = () =>
      this.rgwEndpoints.value.map((endpoint) => ({
        content: endpoint,
        name: endpoint,
        selected: true
      }));
    this.zonegroupEndpointsOptions = makeOptions();
    this.zoneEndpointsOptions = makeOptions();
    const selectedEndpoints = this.rgwEndpoints.value;
    this.multisiteSetupForm.get('zonegroup_endpoints').setValue(selectedEndpoints);
    this.multisiteSetupForm.get('zone_endpoints').setValue(selectedEndpoints);
    ['zonegroup_endpoints', 'zone_endpoints'].forEach((controlName) => {
      this.multisiteSetupForm.get(controlName).valueChanges.subscribe((val: string[]) => {
        if (Array.isArray(val) && val.length === 0) {
          this.multisiteSetupForm.get(controlName).setValue(null, { emitEvent: false });
        }
      });
    });

    this.cdr.detectChanges();
  }

  createForm() {
    this.multisiteSetupForm = new CdFormGroup({
      realmName: new UntypedFormControl('default_realm', {
        validators: [Validators.required]
      }),
      zonegroupName: new UntypedFormControl('default_zonegroup', {
        validators: [Validators.required]
      }),
      zonegroup_endpoints: new UntypedFormControl(null, [Validators.required]),
      zoneName: new UntypedFormControl('default_zone', {
        validators: [Validators.required]
      }),
      zone_endpoints: new UntypedFormControl(null, {
        validators: [Validators.required]
      }),
      username: new UntypedFormControl('default_system_user', {
        validators: [Validators.required]
      }),
      cluster: new UntypedFormControl(null, {
        validators: [Validators.required]
      }),
      replicationZoneName: new UntypedFormControl('new_replicated_zone', {
        validators: [Validators.required]
      }),
      configType: new UntypedFormControl(ConfigType.NewRealm, {}),
      selectedRealm: new UntypedFormControl(null, {}),
      secondary_archive_zone: new UntypedFormControl(false, {})
    });

    if (!this.isMultiClusterConfigured) {
      this.exportTokenForm = new CdFormGroup({});
    }
  }

  get step0Valid(): boolean {
    const f = this.multisiteSetupForm;
    if (f.get('configType').value === 'existingRealm') {
      return f.get('selectedRealm').valid;
    }
    return (
      f.get('realmName').valid && f.get('zonegroupName').valid && f.get('zonegroup_endpoints').valid
    );
  }

  get step1Valid(): boolean {
    const f = this.multisiteSetupForm;
    if (f.get('configType').value === 'existingRealm') {
      return f.get('cluster').valid && f.get('replicationZoneName').valid;
    }
    return f.get('zoneName').valid && f.get('zone_endpoints').valid && f.get('username').valid;
  }

  get step2Valid(): boolean {
    const f = this.multisiteSetupForm;
    return f.get('cluster').valid && f.get('replicationZoneName').valid;
  }

  markStep0Touched(): void {
    const f = this.multisiteSetupForm;
    if (f.get('configType').value === 'existingRealm') {
      ['selectedRealm'].forEach((n) => {
        f.get(n).markAsTouched();
        f.get(n).markAsDirty();
      });
    } else {
      ['realmName', 'zonegroupName', 'zonegroup_endpoints'].forEach((n) => {
        f.get(n).markAsTouched();
        f.get(n).markAsDirty();
      });
    }
  }

  markStep1Touched(): void {
    const f = this.multisiteSetupForm;
    if (f.get('configType').value === 'existingRealm') {
      ['cluster', 'replicationZoneName'].forEach((n) => {
        f.get(n).markAsTouched();
        f.get(n).markAsDirty();
      });
    } else {
      ['zoneName', 'zone_endpoints', 'username'].forEach((n) => {
        f.get(n).markAsTouched();
        f.get(n).markAsDirty();
      });
    }
  }

  markStep2Touched(): void {
    const f = this.multisiteSetupForm;
    ['cluster', 'replicationZoneName'].forEach((n) => {
      f.get(n).markAsTouched();
      f.get(n).markAsDirty();
    });
  }

  showSubmitButtonLabel() {
    if (this.setupCompleted) {
      return $localize`Close`;
    }
    if (this.isMultiClusterConfigured && !this.stepsToSkip['Select cluster']) {
      return $localize`Configure Multi-Site`;
    }
    return $localize`Export Multi-Site token`;
  }

  skipSelectCluster(): void {
    this.stepsToSkip['Select cluster'] = true;
    this.multisiteSetupForm.get('cluster').clearValidators();
    this.multisiteSetupForm.get('cluster').updateValueAndValidity();
    this.multisiteSetupForm.get('replicationZoneName').clearValidators();
    this.multisiteSetupForm.get('replicationZoneName').updateValueAndValidity();
    this.tearsheet?.onNext();
  }

  onValidateStep(event: { step: number }): void {
    const lastStep = this.stepTitles.length - 1;
    if (event.step === 0) this.markStep0Touched();
    else if (event.step === 1) this.markStep1Touched();
    else if (event.step === 2 && event.step !== lastStep) this.markStep2Touched();
  }

  onStepChanged(event: { current: number }): void {
    const selectClusterIndex = this.stepTitles.findIndex((s) => s.label === 'Select cluster');
    if (event.current === selectClusterIndex && this.stepsToSkip['Select cluster']) {
      this.stepsToSkip['Select cluster'] = false;
      this.multisiteSetupForm.get('cluster').setValidators([Validators.required]);
      this.multisiteSetupForm.get('cluster').updateValueAndValidity();
      this.multisiteSetupForm.get('replicationZoneName').setValidators([Validators.required]);
      this.multisiteSetupForm.get('replicationZoneName').updateValueAndValidity();
    }
  }

  onSubmit() {
    if (this.setupCompleted) {
      this.tearsheet.closeTearsheet();
      return;
    }

    this.multisiteSetupForm.markAllAsTouched();
    Object.values(this.multisiteSetupForm.controls).forEach((c) => c.markAsDirty());
    if (this.multisiteSetupForm.invalid) {
      return;
    }

    this.loading = true;

    const proceedWithSetup = () => {
      this.cdr.detectChanges();
      const values = this.multisiteSetupForm.getRawValue();
      const realmName = values['realmName'];
      const zonegroupName = values['zonegroupName'];
      const zonegroupEndpoints = (values['zonegroup_endpoints'] as string[] | string | null)
        ? [].concat(values['zonegroup_endpoints']).join(',')
        : '';
      const zoneName = values['zoneName'];
      const zoneEndpoints = (values['zone_endpoints'] as string[] | string | null)
        ? [].concat(values['zone_endpoints']).join(',')
        : '';
      const username = values['username'];
      if (!this.isMultiClusterConfigured || this.stepsToSkip['Select cluster']) {
        this.rgwMultisiteService
          .setUpMultisiteReplication(
            realmName,
            zonegroupName,
            zonegroupEndpoints,
            zoneName,
            zoneEndpoints,
            username
          )
          .subscribe((data: object) => {
            this.setupCompleted = true;
            this.rgwMultisiteService.setRestartGatewayMessage(false);
            this.loading = false;
            this.realms = (Array.isArray(data) ? data : []).filter(
              (r: object) => r['realm'] === realmName
            );
            this.title = $localize`Multi-site replication created`;
            this.description = $localize`The replication configuration has been created successfully.`;
            this.showSuccessNotification();
          });
      } else {
        const cluster = values['cluster'];
        const replicationZoneName = values['replicationZoneName'];
        const secondaryTierType = values['secondary_archive_zone'] ? 'archive' : '';
        let selectedRealmName = '';

        if (this.multisiteSetupForm.get('configType').value === ConfigType.ExistingRealm) {
          selectedRealmName = this.multisiteSetupForm.get('selectedRealm').value;
        }

        this.rgwMultisiteService
          .setUpMultisiteReplication(
            realmName,
            zonegroupName,
            zonegroupEndpoints,
            zoneName,
            zoneEndpoints,
            username,
            cluster,
            replicationZoneName,
            secondaryTierType,
            this.clusterDetailsArray,
            selectedRealmName
          )
          .subscribe(
            () => {
              this.setupCompleted = true;
              this.rgwMultisiteService.setRestartGatewayMessage(false);
              this.loading = false;
              const selectedClusterDetail = this.clusterDetailsArray.find(
                (c) => c['name'] === values['cluster']
              );
              this.completionSummary = {
                realm:
                  this.multisiteSetupForm.get('configType').value === 'existingRealm'
                    ? values['selectedRealm']
                    : realmName,
                secondaryCluster: selectedClusterDetail
                  ? `${selectedClusterDetail['cluster_alias']} - ${selectedClusterDetail['name']}`
                  : values['cluster'],
                secondaryZone: values['replicationZoneName']
              };
              this.title = $localize`Multi-site replication created`;
              this.description = $localize`The replication configuration has been created successfully.`;
              this.showSuccessNotification();
            },
            () => {
              this.multisiteSetupForm.setErrors({ cdSubmitButton: true });
            }
          );
      }
    };

    if (!this.rgwModuleStatus) {
      this.mgrModuleService.updateModuleState(
        'rgw',
        false,
        undefined,
        '',
        '',
        false,
        $localize`RGW module is being enabled. Waiting for the system to reconnect...`
      );
      const subscription = this.mgrModuleService.updateCompleted$.subscribe(() => {
        subscription.unsubscribe();
        proceedWithSetup();
      });
    } else {
      proceedWithSetup();
    }
  }

  showSuccessNotification() {
    this.notificationService.show(
      NotificationType.success,
      $localize`Multi-site setup completed successfully.`
    );
  }

  closeModal(): void {
    this.location.back();
  }

  onConfigTypeChange(event?: { value: string }) {
    const configType = event?.value ?? this.multisiteSetupForm.get('configType')?.value;
    if (configType === ConfigType.ExistingRealm) {
      this.stepTitles = STEP_TITLES_EXISTING_REALM.map((title) => ({ label: title }));
    } else if (this.isMultiClusterConfigured) {
      this.stepTitles = STEP_TITLES_MULTI_CLUSTER_CONFIGURED.map((title) => ({ label: title }));
    } else {
      this.stepTitles = STEP_TITLES_SINGLE_CLUSTER.map((title) => ({ label: title }));
    }
  }
}
