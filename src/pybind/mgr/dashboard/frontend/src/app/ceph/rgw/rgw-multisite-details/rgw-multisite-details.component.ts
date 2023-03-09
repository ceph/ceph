import { Component, OnDestroy, OnInit, ViewChild } from '@angular/core';
import {
  TreeComponent,
  ITreeOptions,
  TreeModel,
  TreeNode,
  TREE_ACTIONS
} from '@circlon/angular-tree-component';
import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { forkJoin, Subscription } from 'rxjs';
import { RgwRealmService } from '~/app/shared/api/rgw-realm.service';
import { RgwZoneService } from '~/app/shared/api/rgw-zone.service';
import { RgwZonegroupService } from '~/app/shared/api/rgw-zonegroup.service';
import { ActionLabelsI18n, TimerServiceInterval } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';
import { TimerService } from '~/app/shared/services/timer.service';
import { RgwRealm, RgwZone, RgwZonegroup } from '../models/rgw-multisite';
import { RgwMultisiteRealmFormComponent } from '../rgw-multisite-realm-form/rgw-multisite-realm-form.component';
import { RgwMultisiteZoneFormComponent } from '../rgw-multisite-zone-form/rgw-multisite-zone-form.component';
import { RgwMultisiteZonegroupFormComponent } from '../rgw-multisite-zonegroup-form/rgw-multisite-zonegroup-form.component';

@Component({
  selector: 'cd-rgw-multisite-details',
  templateUrl: './rgw-multisite-details.component.html',
  styleUrls: ['./rgw-multisite-details.component.scss']
})
export class RgwMultisiteDetailsComponent implements OnDestroy, OnInit {
  private sub = new Subscription();

  @ViewChild('tree') tree: TreeComponent;

  messages = {
    noDefaultRealm: $localize`Please create a default realm first to enable this feature`
  };

  icons = Icons;
  permission: Permission;
  selection = new CdTableSelection();
  createTableActions: CdTableAction[];
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

  constructor(
    private modalService: ModalService,
    private timerService: TimerService,
    private authStorageService: AuthStorageService,
    public actionLabels: ActionLabelsI18n,
    public timerServiceVariable: TimerServiceInterval,
    public rgwRealmService: RgwRealmService,
    public rgwZonegroupService: RgwZonegroupService,
    public rgwZoneService: RgwZoneService
  ) {
    this.permission = this.authStorageService.getPermissions().rgw;
    const createRealmAction: CdTableAction = {
      permission: 'create',
      icon: Icons.add,
      name: this.actionLabels.CREATE + ' Realm',
      click: () => this.openModal('realm')
    };
    const createZonegroupAction: CdTableAction = {
      permission: 'create',
      icon: Icons.add,
      name: this.actionLabels.CREATE + ' Zonegroup',
      click: () => this.openModal('zonegroup'),
      disable: () => this.getDisable()
    };
    const createZoneAction: CdTableAction = {
      permission: 'create',
      icon: Icons.add,
      name: this.actionLabels.CREATE + ' Zone',
      click: () => this.openModal('zone')
    };
    this.createTableActions = [createRealmAction, createZonegroupAction, createZoneAction];
  }

  openModal(entity: any, edit = false) {
    const entityName = edit ? entity.data.name : entity;
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

  ngOnInit() {
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
  }

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
            const zoneResult = this.rgwZoneService.getZoneTree(zone, this.defaultZoneId, zonegroup);
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
          const zoneResult = this.rgwZoneService.getZoneTree(zone, this.defaultZoneId);
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
    if (this.defaultRealmId === '') {
      return this.messages.noDefaultRealm;
    } else {
      return false;
    }
  }
}
