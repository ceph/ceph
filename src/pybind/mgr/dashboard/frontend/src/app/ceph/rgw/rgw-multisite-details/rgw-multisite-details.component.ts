import {
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  OnDestroy,
  OnInit,
  TemplateRef,
  ViewChild
} from '@angular/core';
import { Node } from 'carbon-components-angular/treeview/tree-node.types';
import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';

import { forkJoin, Subscription } from 'rxjs';
import { RgwRealmService } from '~/app/shared/api/rgw-realm.service';
import { RgwZoneService } from '~/app/shared/api/rgw-zone.service';
import { RgwZonegroupService } from '~/app/shared/api/rgw-zonegroup.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { ActionLabelsI18n, TimerServiceInterval } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { TimerService } from '~/app/shared/services/timer.service';
import { RgwRealm, RgwZone, RgwZonegroup } from '../models/rgw-multisite';
import { RgwMultisiteMigrateComponent } from '../rgw-multisite-migrate/rgw-multisite-migrate.component';
import { RgwMultisiteZoneDeletionFormComponent } from '../models/rgw-multisite-zone-deletion-form/rgw-multisite-zone-deletion-form.component';
import { RgwMultisiteZonegroupDeletionFormComponent } from '../models/rgw-multisite-zonegroup-deletion-form/rgw-multisite-zonegroup-deletion-form.component';
import { RgwMultisiteExportComponent } from '../rgw-multisite-export/rgw-multisite-export.component';
import { RgwMultisiteImportComponent } from '../rgw-multisite-import/rgw-multisite-import.component';
import { RgwMultisiteRealmFormComponent } from '../rgw-multisite-realm-form/rgw-multisite-realm-form.component';
import { RgwMultisiteZoneFormComponent } from '../rgw-multisite-zone-form/rgw-multisite-zone-form.component';
import { RgwMultisiteZonegroupFormComponent } from '../rgw-multisite-zonegroup-form/rgw-multisite-zonegroup-form.component';
import { RgwDaemonService } from '~/app/shared/api/rgw-daemon.service';
import { MgrModuleService } from '~/app/shared/api/mgr-module.service';
import { Router } from '@angular/router';
import { RgwMultisiteWizardComponent } from '../rgw-multisite-wizard/rgw-multisite-wizard.component';
import { RgwMultisiteSyncPolicyComponent } from '../rgw-multisite-sync-policy/rgw-multisite-sync-policy.component';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { RgwMultisiteService } from '~/app/shared/api/rgw-multisite.service';
import { MgrModuleInfo } from '~/app/shared/models/mgr-modules.interface';
import { RGW } from '../utils/constants';

const BASE_URL = 'rgw/multisite/configuration';

@Component({
  selector: 'cd-rgw-multisite-details',
  templateUrl: './rgw-multisite-details.component.html',
  styleUrls: ['./rgw-multisite-details.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class RgwMultisiteDetailsComponent implements OnDestroy, OnInit {
  private sub = new Subscription();
  @ViewChild('treeNodeTemplate') labelTpl: TemplateRef<any>;
  @ViewChild(RgwMultisiteSyncPolicyComponent) syncPolicyComp: RgwMultisiteSyncPolicyComponent;

  messages = {
    noDefaultRealm: $localize`Please create a default realm first to enable this feature`,
    noMasterZone: $localize`Please create a master zone for each zone group to enable this feature`,
    noRealmExists: $localize`No realm exists`,
    disableExport: $localize`Please create master zone group and master zone for each of the realms`
  };

  icons = Icons;
  permissions: Permissions;
  selection = new CdTableSelection();
  createTableActions: CdTableAction[];
  migrateTableAction: CdTableAction[];
  importAction: CdTableAction[];
  exportAction: CdTableAction[];
  multisiteReplicationActions: CdTableAction[];
  loadingIndicator = true;

  toNode(values: any): Node[] {
    return values.map((value: any) => ({
      label: this.labelTpl,
      labelContext: {
        data: { ...value }
      },
      id: value.id,
      value: { ...value },
      expanded: true,
      name: value.name,
      children: value?.children ? this.toNode(value.children) : []
    }));
  }

  set nodes(values: any) {
    this._nodes = this.toNode(values);
    this.changeDetectionRef.detectChanges();
  }

  get nodes() {
    return this._nodes;
  }

  private _nodes: Node[] = [];

  modalRef: NgbModalRef;

  realms: RgwRealm[] = [];
  zonegroups: RgwZonegroup[] = [];
  zones: RgwZone[] = [];
  metadata: any;
  metadataTitle: string;
  bsModalRef: NgbModalRef;
  realmIds: string[] = [];
  zoneIds: string[] = [];
  defaultRealmId = '';
  defaultZonegroupId = '';
  defaultZoneId = '';
  multisiteInfo: object[] = [];
  defaultsInfo: string[] = [];
  showMigrateAndReplicationActions = false;
  editTitle: string = 'Edit';
  deleteTitle: string = 'Delete';
  disableExport = true;
  rgwModuleStatus: boolean;
  restartGatewayMessage = false;
  rgwModuleData: string | any[] = [];
  activeId: string;
  activeNodeId?: string;
  MODULE_NAME = 'rgw';
  NAVIGATE_TO = '/rgw/multisite';

  constructor(
    private modalService: ModalService,
    private timerService: TimerService,
    private authStorageService: AuthStorageService,
    public actionLabels: ActionLabelsI18n,
    public timerServiceVariable: TimerServiceInterval,
    public router: Router,
    public rgwRealmService: RgwRealmService,
    public rgwZonegroupService: RgwZonegroupService,
    public rgwZoneService: RgwZoneService,
    public rgwDaemonService: RgwDaemonService,
    public mgrModuleService: MgrModuleService,
    private notificationService: NotificationService,
    private cdsModalService: ModalCdsService,
    private rgwMultisiteService: RgwMultisiteService,
    private changeDetectionRef: ChangeDetectorRef
  ) {
    this.permissions = this.authStorageService.getPermissions();
  }

  openModal(entity: any | string, edit = false) {
    const entityName = edit ? entity?.data?.type : entity;
    const action = edit ? 'edit' : 'create';
    const initialState = {
      resource: entityName,
      action: action,
      info: entity,
      defaultsInfo: this.defaultsInfo,
      multisiteInfo: this.multisiteInfo
    };
    if (entityName === 'realm') {
      this.bsModalRef = this.cdsModalService.show(RgwMultisiteRealmFormComponent, initialState);
    } else if (entityName === 'zonegroup') {
      this.bsModalRef = this.modalService.show(RgwMultisiteZonegroupFormComponent, initialState, {
        size: 'lg'
      });
    } else {
      this.bsModalRef = this.modalService.show(RgwMultisiteZoneFormComponent, initialState, {
        size: 'lg'
      });
    }
  }

  openMultisiteSetupWizard() {
    this.bsModalRef = this.cdsModalService.show(RgwMultisiteWizardComponent);
  }

  openMigrateModal() {
    const initialState = {
      multisiteInfo: this.multisiteInfo
    };
    this.bsModalRef = this.modalService.show(RgwMultisiteMigrateComponent, initialState, {
      size: 'lg'
    });
  }

  openImportModal() {
    const initialState = {
      multisiteInfo: this.multisiteInfo
    };
    this.bsModalRef = this.modalService.show(RgwMultisiteImportComponent, initialState, {
      size: 'lg'
    });
  }

  openExportModal() {
    const initialState = {
      defaultsInfo: this.defaultsInfo,
      multisiteInfo: this.multisiteInfo
    };
    this.bsModalRef = this.modalService.show(RgwMultisiteExportComponent, initialState, {
      size: 'lg'
    });
  }

  getDisableExport() {
    this.realms.forEach((realm: any) => {
      this.zonegroups.forEach((zonegroup) => {
        if (realm.id === zonegroup.realm_id) {
          if (zonegroup.is_master && zonegroup.master_zone !== '') {
            this.disableExport = false;
          }
        }
      });
    });
    if (!this.rgwModuleStatus) {
      return true;
    }
    if (this.realms.length < 1) {
      return this.messages.noRealmExists;
    } else if (this.disableExport) {
      return this.messages.disableExport;
    } else {
      return false;
    }
  }

  getDisableImport() {
    if (!this.rgwModuleStatus) {
      return true;
    } else {
      return false;
    }
  }

  ngOnInit() {
    this.createTableActions = [
      {
        permission: 'create',
        icon: Icons.add,
        name: this.actionLabels.CREATE + ' Realm',
        click: () => this.openModal('realm')
      },
      {
        permission: 'create',
        icon: Icons.add,
        name: this.actionLabels.CREATE + ' Zone Group',
        click: () => this.openModal('zonegroup'),
        disable: () => this.getDisable()
      },
      {
        permission: 'create',
        icon: Icons.add,
        name: this.actionLabels.CREATE + ' Zone',
        click: () => this.openModal('zone')
      }
    ];
    this.migrateTableAction = [
      {
        permission: 'create',
        icon: Icons.wrench,
        name: this.actionLabels.MIGRATE,
        click: () => this.openMigrateModal()
      }
    ];
    this.importAction = [
      {
        permission: 'create',
        icon: Icons.download,
        name: this.actionLabels.IMPORT,
        click: () => this.openImportModal(),
        disable: () => this.getDisableImport()
      }
    ];
    this.exportAction = [
      {
        permission: 'create',
        icon: Icons.upload,
        name: this.actionLabels.EXPORT,
        click: () => this.openExportModal(),
        disable: () => this.getDisableExport()
      }
    ];
    this.multisiteReplicationActions = [
      {
        permission: 'create',
        icon: Icons.wrench,
        name: this.actionLabels.SETUP_MULTISITE_REPLICATION,
        click: () =>
          this.router.navigate([BASE_URL, { outlets: { modal: 'setup-multisite-replication' } }])
      }
    ];

    const observables = [
      this.rgwRealmService.getAllRealmsInfo(),
      this.rgwZonegroupService.getAllZonegroupsInfo(),
      this.rgwZoneService.getAllZonesInfo()
    ];
    this.sub = this.timerService
      .get(() => forkJoin(observables), this.timerServiceVariable.TIMER_SERVICE_PERIOD * 2)
      .subscribe(
        (multisiteInfo: [object, object, object]) => {
          this.multisiteInfo = multisiteInfo;
          this.loadingIndicator = false;
          this.nodes = this.abstractTreeData(multisiteInfo);
        },
        (_error) => {}
      );

    // Only get the module status if you can read from configOpt
    if (this.permissions.configOpt.read) this.getRgwModuleStatus();
  }

  ngOnDestroy() {
    this.sub.unsubscribe();
  }

  private getRgwModuleStatus() {
    this.mgrModuleService.list().subscribe((moduleData: MgrModuleInfo[]) => {
      this.rgwModuleData = moduleData.filter((module: MgrModuleInfo) => module.name === RGW);
      if (this.rgwModuleData.length > 0) {
        this.rgwModuleStatus = this.rgwModuleData[0].enabled;
      }
    });
  }

  private abstractTreeData(multisiteInfo: [object, object, object]): any[] {
    let allNodes: object[] = [];
    let rootNodes = {};
    let firstChildNodes = {};
    let allFirstChildNodes = [];
    let secondChildNodes = {};
    let allSecondChildNodes: {}[] = [];
    this.realms = multisiteInfo[0]['realms'];
    this.zonegroups = multisiteInfo[1]['zonegroups'];
    this.zones = multisiteInfo[2]['zones'];
    this.defaultRealmId = multisiteInfo[0]['default_realm'];
    this.defaultZonegroupId = multisiteInfo[1]['default_zonegroup'];
    this.defaultZoneId = multisiteInfo[2]['default_zone'];
    this.defaultsInfo = this.getDefaultsEntities(
      this.defaultRealmId,
      this.defaultZonegroupId,
      this.defaultZoneId
    );
    if (this.realms.length > 0) {
      // get tree for realm -> zonegroup -> zone
      for (const realm of this.realms) {
        const result = this.rgwRealmService.getRealmTree(realm, this.defaultRealmId);
        rootNodes = result['nodes'];
        this.realmIds = this.realmIds.concat(result['realmIds']);
        for (const zonegroup of this.zonegroups) {
          if (zonegroup.realm_id === realm.id) {
            firstChildNodes = this.rgwZonegroupService.getZonegroupTree(
              zonegroup,
              this.defaultZonegroupId,
              realm
            );
            for (const zone of zonegroup.zones) {
              const zoneResult = this.rgwZoneService.getZoneTree(
                zone,
                this.defaultZoneId,
                this.zones,
                zonegroup,
                realm
              );
              secondChildNodes = zoneResult['nodes'];
              this.zoneIds = this.zoneIds.concat(zoneResult['zoneIds']);
              allSecondChildNodes.push(secondChildNodes);
              secondChildNodes = {};
            }
            allSecondChildNodes = allSecondChildNodes.map((x) => ({
              ...x,
              parentNode: firstChildNodes
            }));
            firstChildNodes['children'] = allSecondChildNodes;
            allSecondChildNodes = [];
            allFirstChildNodes.push(firstChildNodes);
            firstChildNodes = {};
          }
        }
        allFirstChildNodes = allFirstChildNodes.map((x) => ({ ...x, parentNode: rootNodes }));
        rootNodes['children'] = allFirstChildNodes;
        allNodes.push({ ...rootNodes, label: rootNodes?.['name'] || rootNodes?.['id'] });
        firstChildNodes = {};
        secondChildNodes = {};
        rootNodes = {};
        allFirstChildNodes = [];
        allSecondChildNodes = [];
      }
    }
    if (this.zonegroups.length > 0) {
      // get tree for zonegroup -> zone (standalone zonegroups that don't match a realm eg(initial default))
      for (const zonegroup of this.zonegroups) {
        if (!this.realmIds.includes(zonegroup.realm_id)) {
          rootNodes = this.rgwZonegroupService.getZonegroupTree(zonegroup, this.defaultZonegroupId);
          for (const zone of zonegroup.zones) {
            const zoneResult = this.rgwZoneService.getZoneTree(
              zone,
              this.defaultZoneId,
              this.zones,
              zonegroup
            );
            firstChildNodes = zoneResult['nodes'];
            this.zoneIds = this.zoneIds.concat(zoneResult['zoneIds']);
            allFirstChildNodes.push(firstChildNodes);
            firstChildNodes = {};
          }
          allFirstChildNodes = allFirstChildNodes.map((x) => ({ ...x, parentNode: rootNodes }));
          rootNodes['children'] = allFirstChildNodes;
          allNodes.push({ ...rootNodes, label: rootNodes?.['name'] || rootNodes?.['id'] });
          firstChildNodes = {};
          rootNodes = {};
          allFirstChildNodes = [];
        }
      }
    }
    if (this.zones.length > 0) {
      // get tree for standalone zones(zones that do not belong to a zonegroup)
      for (const zone of this.zones) {
        if (this.zoneIds.length > 0 && !this.zoneIds.includes(zone.id)) {
          const zoneResult = this.rgwZoneService.getZoneTree(zone, this.defaultZoneId, this.zones);
          rootNodes = zoneResult['nodes'];
          allNodes.push({ ...rootNodes, label: rootNodes?.['name'] || rootNodes?.['id'] });
          rootNodes = {};
        }
      }
    }
    if (this.realms.length < 1 && this.zonegroups.length < 1 && this.zones.length < 1) {
      return [
        {
          name: 'No nodes!',
          label: 'No nodes!'
        }
      ];
    }
    this.realmIds = [];
    this.zoneIds = [];
    this.evaluateMigrateAndReplicationActions();
    this.rgwMultisiteService.restartGatewayMessage$.subscribe((value) => {
      if (value !== null) {
        this.restartGatewayMessage = value;
      } else {
        this.checkRestartGatewayMessage();
      }
    });
    return allNodes;
  }

  checkRestartGatewayMessage() {
    this.rgwDaemonService.list().subscribe((data: any) => {
      const realmName = data.map((item: { [x: string]: any }) => item['realm_name']);
      if (
        this.defaultRealmId !== '' &&
        this.defaultZonegroupId !== '' &&
        this.defaultZoneId !== '' &&
        realmName.includes('')
      ) {
        this.restartGatewayMessage = true;
      } else {
        this.restartGatewayMessage = false;
      }
    });
  }

  getDefaultsEntities(
    defaultRealmId: string,
    defaultZonegroupId: string,
    defaultZoneId: string
  ): any {
    const defaultRealm = this.realms?.find((x: { id: string }) => x.id === defaultRealmId);
    const defaultZonegroup = this.zonegroups?.find(
      (x: { id: string }) => x.id === defaultZonegroupId
    );
    const defaultZone = this.zones?.find((x: { id: string }) => x.id === defaultZoneId);

    return {
      defaultRealmName: defaultRealm?.name,
      defaultZonegroupName: defaultZonegroup?.name,
      defaultZoneName: defaultZone?.name
    };
  }

  onNodeSelected(node: Node) {
    this.metadataTitle = node?.value?.name;
    this.metadata = node?.value?.info;
    this.activeNodeId = node?.value?.id;
    node.expanded = true;
  }

  getDisable() {
    let isMasterZone = true;
    if (this.defaultRealmId === '') {
      return this.messages.noDefaultRealm;
    } else {
      this.zonegroups.forEach((zgp: any) => {
        if (_.isEmpty(zgp.master_zone)) {
          isMasterZone = false;
        }
      });
      if (!isMasterZone) {
        setTimeout(() => {
          this.editTitle =
            'Please create a master zone for each existing zonegroup to enable this feature';
        }, 1);
        return this.messages.noMasterZone;
      } else {
        setTimeout(() => {
          this.editTitle = 'Edit';
        }, 1);
        return false;
      }
    }
  }

  evaluateMigrateAndReplicationActions() {
    if (
      this.realms.length === 0 &&
      this.zonegroups.length === 1 &&
      this.zonegroups[0].name === 'default' &&
      this.zones.length === 1 &&
      this.zones[0].name === 'default'
    ) {
      this.showMigrateAndReplicationActions = true;
    } else {
      this.showMigrateAndReplicationActions = false;
    }
    return this.showMigrateAndReplicationActions;
  }

  isDeleteDisabled(node: Node): { isDisabled: boolean; deleteTitle: string } {
    let isDisabled: boolean = false;
    let deleteTitle: string = this.deleteTitle;
    let masterZonegroupCount: number = 0;
    if (node?.value?.type === 'realm' && node?.data?.is_default && this.realms.length < 2) {
      isDisabled = true;
    }

    if (node?.data?.type === 'zonegroup') {
      if (this.zonegroups.length < 2) {
        deleteTitle = 'You can not delete the only zonegroup available';
        isDisabled = true;
      } else if (node?.data?.is_default) {
        deleteTitle = 'You can not delete the default zonegroup';
        isDisabled = true;
      } else if (node?.data?.is_master) {
        for (let zonegroup of this.zonegroups) {
          if (zonegroup.is_master === true) {
            masterZonegroupCount++;
            if (masterZonegroupCount > 1) break;
          }
        }
        if (masterZonegroupCount < 2) {
          deleteTitle = 'You can not delete the only master zonegroup available';
          isDisabled = true;
        }
      }
    }

    if (node?.data?.type === 'zone') {
      if (this.zones.length < 2) {
        deleteTitle = 'You can not delete the only zone available';
        isDisabled = true;
      } else if (node?.data?.is_default) {
        deleteTitle = 'You can not delete the default zone';
        isDisabled = true;
      } else if (node?.data?.is_master && node?.data?.zone_zonegroup.zones.length < 2) {
        deleteTitle =
          'You can not delete the master zone as there are no more zones in this zonegroup';
        isDisabled = true;
      }
    }

    if (!isDisabled) {
      this.deleteTitle = 'Delete';
    }

    return { isDisabled, deleteTitle };
  }

  delete(node: Node) {
    if (node?.data?.type === 'realm') {
      const modalRef = this.cdsModalService.show(DeleteConfirmationModalComponent, {
        itemDescription: $localize`${node?.data?.type} ${node?.data?.name}`,
        itemNames: [`${node?.data?.name}`],
        submitAction: () => {
          this.rgwRealmService.delete(node?.data?.name).subscribe(
            () => {
              this.notificationService.show(
                NotificationType.success,
                $localize`Realm: '${node?.data?.name}' deleted successfully`
              );
              this.cdsModalService.dismissAll();
            },
            () => {
              this.cdsModalService.stopLoadingSpinner(modalRef.deletionForm);
            }
          );
        }
      });
    } else if (node?.data?.type === 'zonegroup') {
      this.modalRef = this.modalService.show(RgwMultisiteZonegroupDeletionFormComponent, {
        zonegroup: node.data
      });
    } else if (node?.data?.type === 'zone') {
      this.modalRef = this.modalService.show(RgwMultisiteZoneDeletionFormComponent, {
        zone: node.data
      });
    }
  }

  enableRgwModule() {
    this.mgrModuleService.updateModuleState(
      this.MODULE_NAME,
      false,
      null,
      this.NAVIGATE_TO,
      'Enabled RGW Module',
      true
    );
  }
}
