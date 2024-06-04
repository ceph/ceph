import { Component, OnDestroy, OnInit, ViewChild } from '@angular/core';
import {
  TreeComponent,
  ITreeOptions,
  TreeModel,
  TreeNode,
  TREE_ACTIONS
} from '@circlon/angular-tree-component';
import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';

import { forkJoin, Subscription, timer as observableTimer } from 'rxjs';
import { RgwRealmService } from '~/app/shared/api/rgw-realm.service';
import { RgwZoneService } from '~/app/shared/api/rgw-zone.service';
import { RgwZonegroupService } from '~/app/shared/api/rgw-zonegroup.service';
import { CriticalConfirmationModalComponent } from '~/app/shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { ActionLabelsI18n, TimerServiceInterval } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permission } from '~/app/shared/models/permissions';
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
import { BlockUI, NgBlockUI } from 'ng-block-ui';
import { Router } from '@angular/router';

@Component({
  selector: 'cd-rgw-multisite-details',
  templateUrl: './rgw-multisite-details.component.html',
  styleUrls: ['./rgw-multisite-details.component.scss']
})
export class RgwMultisiteDetailsComponent implements OnDestroy, OnInit {
  private sub = new Subscription();

  @ViewChild('tree') tree: TreeComponent;

  messages = {
    noDefaultRealm: $localize`Please create a default realm first to enable this feature`,
    noMasterZone: $localize`Please create a master zone for each zone group to enable this feature`,
    noRealmExists: $localize`No realm exists`,
    disableExport: $localize`Please create master zone group and master zone for each of the realms`
  };

  @BlockUI()
  blockUI: NgBlockUI;

  icons = Icons;
  permission: Permission;
  selection = new CdTableSelection();
  createTableActions: CdTableAction[];
  migrateTableAction: CdTableAction[];
  importAction: CdTableAction[];
  exportAction: CdTableAction[];
  loadingIndicator = true;
  nodes: object[] = [];
  treeOptions: ITreeOptions = {
    useVirtualScroll: true,
    nodeHeight: 22,
    levelPadding: 20,
    actionMapping: {
      mouse: {
        click: this.onNodeSelected.bind(this)
      }
    }
  };
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
  showMigrateAction: boolean = false;
  editTitle: string = 'Edit';
  deleteTitle: string = 'Delete';
  disableExport = true;
  rgwModuleStatus: boolean;
  restartGatewayMessage = false;
  rgwModuleData: string | any[] = [];

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
    private notificationService: NotificationService
  ) {
    this.permission = this.authStorageService.getPermissions().rgw;
  }

  openModal(entity: any, edit = false) {
    const entityName = edit ? entity.data.type : entity;
    const action = edit ? 'edit' : 'create';
    const initialState = {
      resource: entityName,
      action: action,
      info: entity,
      defaultsInfo: this.defaultsInfo,
      multisiteInfo: this.multisiteInfo
    };
    if (entityName === 'realm') {
      this.bsModalRef = this.modalService.show(RgwMultisiteRealmFormComponent, initialState, {
        size: 'lg'
      });
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
    const createRealmAction: CdTableAction = {
      permission: 'create',
      icon: Icons.add,
      name: this.actionLabels.CREATE + ' Realm',
      click: () => this.openModal('realm')
    };
    const createZonegroupAction: CdTableAction = {
      permission: 'create',
      icon: Icons.add,
      name: this.actionLabels.CREATE + ' Zone Group',
      click: () => this.openModal('zonegroup'),
      disable: () => this.getDisable()
    };
    const createZoneAction: CdTableAction = {
      permission: 'create',
      icon: Icons.add,
      name: this.actionLabels.CREATE + ' Zone',
      click: () => this.openModal('zone')
    };
    const migrateMultsiteAction: CdTableAction = {
      permission: 'read',
      icon: Icons.exchange,
      name: this.actionLabels.MIGRATE,
      click: () => this.openMigrateModal()
    };
    const importMultsiteAction: CdTableAction = {
      permission: 'read',
      icon: Icons.download,
      name: this.actionLabels.IMPORT,
      click: () => this.openImportModal(),
      disable: () => this.getDisableImport()
    };
    const exportMultsiteAction: CdTableAction = {
      permission: 'read',
      icon: Icons.upload,
      name: this.actionLabels.EXPORT,
      click: () => this.openExportModal(),
      disable: () => this.getDisableExport()
    };
    this.createTableActions = [createRealmAction, createZonegroupAction, createZoneAction];
    this.migrateTableAction = [migrateMultsiteAction];
    this.importAction = [importMultsiteAction];
    this.exportAction = [exportMultsiteAction];

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
    this.mgrModuleService.list().subscribe((moduleData: any) => {
      this.rgwModuleData = moduleData.filter((module: object) => module['name'] === 'rgw');
      if (this.rgwModuleData.length > 0) {
        this.rgwModuleStatus = this.rgwModuleData[0].enabled;
      }
    });
  }

  /* setConfigValues() {
    this.rgwDaemonService
      .setMultisiteConfig(
        this.defaultsInfo['defaultRealmName'],
        this.defaultsInfo['defaultZonegroupName'],
        this.defaultsInfo['defaultZoneName']
      )
      .subscribe(() => {});
  }*/

  ngOnDestroy() {
    this.sub.unsubscribe();
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
            firstChildNodes['children'] = allSecondChildNodes;
            allSecondChildNodes = [];
            allFirstChildNodes.push(firstChildNodes);
            firstChildNodes = {};
          }
        }
        rootNodes['children'] = allFirstChildNodes;
        allNodes.push(rootNodes);
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
          rootNodes['children'] = allFirstChildNodes;
          allNodes.push(rootNodes);
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
          allNodes.push(rootNodes);
          rootNodes = {};
        }
      }
    }
    if (this.realms.length < 1 && this.zonegroups.length < 1 && this.zones.length < 1) {
      return [
        {
          name: 'No nodes!'
        }
      ];
    }
    this.realmIds = [];
    this.zoneIds = [];
    this.getDisableMigrate();
    this.rgwDaemonService.list().subscribe((data: any) => {
      const realmName = data.map((item: { [x: string]: any }) => item['realm_name']);
      if (
        this.defaultRealmId != '' &&
        this.defaultZonegroupId != '' &&
        this.defaultZoneId != '' &&
        realmName.includes('')
      ) {
        this.restartGatewayMessage = true;
      }
    });
    return allNodes;
  }

  getDefaultsEntities(
    defaultRealmId: string,
    defaultZonegroupId: string,
    defaultZoneId: string
  ): any {
    const defaultRealm = this.realms.find((x: { id: string }) => x.id === defaultRealmId);
    const defaultZonegroup = this.zonegroups.find(
      (x: { id: string }) => x.id === defaultZonegroupId
    );
    const defaultZone = this.zones.find((x: { id: string }) => x.id === defaultZoneId);
    const defaultRealmName = defaultRealm !== undefined ? defaultRealm.name : null;
    const defaultZonegroupName = defaultZonegroup !== undefined ? defaultZonegroup.name : null;
    const defaultZoneName = defaultZone !== undefined ? defaultZone.name : null;
    return {
      defaultRealmName: defaultRealmName,
      defaultZonegroupName: defaultZonegroupName,
      defaultZoneName: defaultZoneName
    };
  }

  onNodeSelected(tree: TreeModel, node: TreeNode) {
    TREE_ACTIONS.ACTIVATE(tree, node, true);
    this.metadataTitle = node.data.name;
    this.metadata = node.data.info;
    node.data.show = true;
  }

  onUpdateData() {
    this.tree.treeModel.expandAll();
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
        this.editTitle =
          'Please create a master zone for each existing zonegroup to enable this feature';
        return this.messages.noMasterZone;
      } else {
        this.editTitle = 'Edit';
        return false;
      }
    }
  }

  getDisableMigrate() {
    if (
      this.realms.length === 0 &&
      this.zonegroups.length === 1 &&
      this.zonegroups[0].name === 'default' &&
      this.zones.length === 1 &&
      this.zones[0].name === 'default'
    ) {
      this.showMigrateAction = true;
    } else {
      this.showMigrateAction = false;
    }
    return this.showMigrateAction;
  }

  isDeleteDisabled(node: TreeNode): boolean {
    let disable: boolean = false;
    let masterZonegroupCount: number = 0;
    if (node.data.type === 'realm' && node.data.is_default && this.realms.length < 2) {
      disable = true;
    }

    if (node.data.type === 'zonegroup') {
      if (this.zonegroups.length < 2) {
        this.deleteTitle = 'You can not delete the only zonegroup available';
        disable = true;
      } else if (node.data.is_default) {
        this.deleteTitle = 'You can not delete the default zonegroup';
        disable = true;
      } else if (node.data.is_master) {
        for (let zonegroup of this.zonegroups) {
          if (zonegroup.is_master === true) {
            masterZonegroupCount++;
            if (masterZonegroupCount > 1) break;
          }
        }
        if (masterZonegroupCount < 2) {
          this.deleteTitle = 'You can not delete the only master zonegroup available';
          disable = true;
        }
      }
    }

    if (node.data.type === 'zone') {
      if (this.zones.length < 2) {
        this.deleteTitle = 'You can not delete the only zone available';
        disable = true;
      } else if (node.data.is_default) {
        this.deleteTitle = 'You can not delete the default zone';
        disable = true;
      } else if (node.data.is_master && node.data.zone_zonegroup.zones.length < 2) {
        this.deleteTitle =
          'You can not delete the master zone as there are no more zones in this zonegroup';
        disable = true;
      }
    }

    if (!disable) {
      this.deleteTitle = 'Delete';
    }

    return disable;
  }

  delete(node: TreeNode) {
    if (node.data.type === 'realm') {
      this.modalRef = this.modalService.show(CriticalConfirmationModalComponent, {
        itemDescription: $localize`${node.data.type} ${node.data.name}`,
        itemNames: [`${node.data.name}`],
        submitAction: () => {
          this.rgwRealmService.delete(node.data.name).subscribe(
            () => {
              this.modalRef.close();
              this.notificationService.show(
                NotificationType.success,
                $localize`Realm: '${node.data.name}' deleted successfully`
              );
            },
            () => {
              this.modalRef.componentInstance.stopLoadingSpinner();
            }
          );
        }
      });
    } else if (node.data.type === 'zonegroup') {
      this.modalRef = this.modalService.show(RgwMultisiteZonegroupDeletionFormComponent, {
        zonegroup: node.data
      });
    } else if (node.data.type === 'zone') {
      this.modalRef = this.modalService.show(RgwMultisiteZoneDeletionFormComponent, {
        zone: node.data
      });
    }
  }

  enableRgwModule() {
    let $obs;
    const fnWaitUntilReconnected = () => {
      observableTimer(2000).subscribe(() => {
        // Trigger an API request to check if the connection is
        // re-established.
        this.mgrModuleService.list().subscribe(
          () => {
            // Resume showing the notification toasties.
            this.notificationService.suspendToasties(false);
            // Unblock the whole UI.
            this.blockUI.stop();
            // Reload the data table content.
            this.notificationService.show(NotificationType.success, $localize`Enabled RGW Module`);
            this.router.navigateByUrl('/', { skipLocationChange: true }).then(() => {
              this.router.navigate(['/rgw/multisite']);
            });
            // Reload the data table content.
          },
          () => {
            fnWaitUntilReconnected();
          }
        );
      });
    };

    if (!this.rgwModuleStatus) {
      $obs = this.mgrModuleService.enable('rgw');
    }
    $obs.subscribe(
      () => undefined,
      () => {
        // Suspend showing the notification toasties.
        this.notificationService.suspendToasties(true);
        // Block the whole UI to prevent user interactions until
        // the connection to the backend is reestablished
        this.blockUI.start($localize`Reconnecting, please wait ...`);
        fnWaitUntilReconnected();
      }
    );
  }
}
