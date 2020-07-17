import { Component, OnInit } from '@angular/core';
import { FormArray, FormControl, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';

import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import * as _ from 'lodash';
import { forkJoin } from 'rxjs';

import { IscsiService } from '../../../shared/api/iscsi.service';
import { RbdService } from '../../../shared/api/rbd.service';
import { SelectMessages } from '../../../shared/components/select/select-messages.model';
import { SelectOption } from '../../../shared/components/select/select-option.model';
import { ActionLabelsI18n } from '../../../shared/constants/app.constants';
import { Icons } from '../../../shared/enum/icons.enum';
import { CdForm } from '../../../shared/forms/cd-form';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';
import { CdValidators } from '../../../shared/forms/cd-validators';
import { FinishedTask } from '../../../shared/models/finished-task';
import { ModalService } from '../../../shared/services/modal.service';
import { TaskWrapperService } from '../../../shared/services/task-wrapper.service';
import { IscsiTargetImageSettingsModalComponent } from '../iscsi-target-image-settings-modal/iscsi-target-image-settings-modal.component';
import { IscsiTargetIqnSettingsModalComponent } from '../iscsi-target-iqn-settings-modal/iscsi-target-iqn-settings-modal.component';

@Component({
  selector: 'cd-iscsi-target-form',
  templateUrl: './iscsi-target-form.component.html',
  styleUrls: ['./iscsi-target-form.component.scss']
})
export class IscsiTargetFormComponent extends CdForm implements OnInit {
  cephIscsiConfigVersion: number;
  targetForm: CdFormGroup;
  modalRef: NgbModalRef;
  api_version = 0;
  minimum_gateways = 1;
  target_default_controls: any;
  target_controls_limits: any;
  disk_default_controls: any;
  disk_controls_limits: any;
  backstores: string[];
  default_backstore: string;
  unsupported_rbd_features: any;
  required_rbd_features: any;

  icons = Icons;

  isEdit = false;
  target_iqn: string;

  imagesAll: any[];
  imagesSelections: SelectOption[];
  portalsSelections: SelectOption[] = [];

  imagesInitiatorSelections: SelectOption[][] = [];
  groupDiskSelections: SelectOption[][] = [];
  groupMembersSelections: SelectOption[][] = [];

  imagesSettings: any = {};
  messages = {
    portals: new SelectMessages({ noOptions: $localize`There are no portals available.` }),
    images: new SelectMessages({ noOptions: $localize`There are no images available.` }),
    initiatorImage: new SelectMessages({
      noOptions: $localize`There are no images available. Please make sure you add an image to the target.`
    }),
    groupInitiator: new SelectMessages({
      noOptions: $localize`There are no initiators available. Please make sure you add an initiator to the target.`
    })
  };

  IQN_REGEX = /^iqn\.(19|20)\d\d-(0[1-9]|1[0-2])\.\D{2,3}(\.[A-Za-z0-9-]+)+(:[A-Za-z0-9-\.]+)*$/;
  USER_REGEX = /^[\w\.:@_-]{8,64}$/;
  PASSWORD_REGEX = /^[\w@\-_\/]{12,16}$/;
  action: string;
  resource: string;

  constructor(
    private iscsiService: IscsiService,
    private modalService: ModalService,
    private rbdService: RbdService,
    private router: Router,
    private route: ActivatedRoute,
    private taskWrapper: TaskWrapperService,
    public actionLabels: ActionLabelsI18n
  ) {
    super();
    this.resource = $localize`target`;
  }

  ngOnInit() {
    const promises: any[] = [
      this.iscsiService.listTargets(),
      this.rbdService.list(),
      this.iscsiService.portals(),
      this.iscsiService.settings(),
      this.iscsiService.version()
    ];

    if (this.router.url.startsWith('/block/iscsi/targets/edit')) {
      this.isEdit = true;
      this.route.params.subscribe((params: { target_iqn: string }) => {
        this.target_iqn = decodeURIComponent(params.target_iqn);
        promises.push(this.iscsiService.getTarget(this.target_iqn));
      });
    }
    this.action = this.isEdit ? this.actionLabels.EDIT : this.actionLabels.CREATE;

    forkJoin(promises).subscribe((data: any[]) => {
      // iscsiService.listTargets
      const usedImages = _(data[0])
        .filter((target) => target.target_iqn !== this.target_iqn)
        .flatMap((target) => target.disks)
        .map((image) => `${image.pool}/${image.image}`)
        .value();

      // iscsiService.settings()
      if ('api_version' in data[3]) {
        this.api_version = data[3].api_version;
      }
      this.minimum_gateways = data[3].config.minimum_gateways;
      this.target_default_controls = data[3].target_default_controls;
      this.target_controls_limits = data[3].target_controls_limits;
      this.disk_default_controls = data[3].disk_default_controls;
      this.disk_controls_limits = data[3].disk_controls_limits;
      this.backstores = data[3].backstores;
      this.default_backstore = data[3].default_backstore;
      this.unsupported_rbd_features = data[3].unsupported_rbd_features;
      this.required_rbd_features = data[3].required_rbd_features;

      // rbdService.list()
      this.imagesAll = _(data[1])
        .flatMap((pool) => pool.value)
        .filter((image) => {
          // Namespaces are not supported by ceph-iscsi
          if (image.namespace) {
            return false;
          }
          const imageId = `${image.pool_name}/${image.name}`;
          if (usedImages.indexOf(imageId) !== -1) {
            return false;
          }
          const validBackstores = this.getValidBackstores(image);
          if (validBackstores.length === 0) {
            return false;
          }
          return true;
        })
        .value();

      this.imagesSelections = this.imagesAll.map(
        (image) => new SelectOption(false, `${image.pool_name}/${image.name}`, '')
      );

      // iscsiService.portals()
      const portals: SelectOption[] = [];
      data[2].forEach((portal: Record<string, any>) => {
        portal.ip_addresses.forEach((ip: string) => {
          portals.push(new SelectOption(false, portal.name + ':' + ip, ''));
        });
      });
      this.portalsSelections = [...portals];

      // iscsiService.version()
      this.cephIscsiConfigVersion = data[4]['ceph_iscsi_config_version'];

      this.createForm();

      // iscsiService.getTarget()
      if (data[5]) {
        this.resolveModel(data[5]);
      }

      this.loadingReady();
    });
  }

  createForm() {
    this.targetForm = new CdFormGroup({
      target_iqn: new FormControl('iqn.2001-07.com.ceph:' + Date.now(), {
        validators: [Validators.required, Validators.pattern(this.IQN_REGEX)]
      }),
      target_controls: new FormControl({}),
      portals: new FormControl([], {
        validators: [
          CdValidators.custom('minGateways', (value: any[]) => {
            const gateways = _.uniq(value.map((elem) => elem.split(':')[0]));
            return gateways.length < Math.max(1, this.minimum_gateways);
          })
        ]
      }),
      disks: new FormControl([], {
        validators: [
          CdValidators.custom('dupLunId', (value: any[]) => {
            const lunIds = this.getLunIds(value);
            return lunIds.length !== _.uniq(lunIds).length;
          }),
          CdValidators.custom('dupWwn', (value: any[]) => {
            const wwns = this.getWwns(value);
            return wwns.length !== _.uniq(wwns).length;
          })
        ]
      }),
      initiators: new FormArray([]),
      groups: new FormArray([]),
      acl_enabled: new FormControl(false)
    });
    // Target level authentication was introduced in ceph-iscsi config v11
    if (this.cephIscsiConfigVersion > 10) {
      const authFormGroup = new CdFormGroup({
        user: new FormControl(''),
        password: new FormControl(''),
        mutual_user: new FormControl(''),
        mutual_password: new FormControl('')
      });
      this.setAuthValidator(authFormGroup);
      this.targetForm.addControl('auth', authFormGroup);
    }
  }

  resolveModel(res: Record<string, any>) {
    this.targetForm.patchValue({
      target_iqn: res.target_iqn,
      target_controls: res.target_controls,
      acl_enabled: res.acl_enabled
    });
    // Target level authentication was introduced in ceph-iscsi config v11
    if (this.cephIscsiConfigVersion > 10) {
      this.targetForm.patchValue({
        auth: res.auth
      });
    }
    const portals: any[] = [];
    _.forEach(res.portals, (portal) => {
      const id = `${portal.host}:${portal.ip}`;
      portals.push(id);
    });
    this.targetForm.patchValue({
      portals: portals
    });

    const disks: any[] = [];
    _.forEach(res.disks, (disk) => {
      const id = `${disk.pool}/${disk.image}`;
      disks.push(id);
      this.imagesSettings[id] = {
        backstore: disk.backstore
      };
      this.imagesSettings[id][disk.backstore] = disk.controls;
      if ('lun' in disk) {
        this.imagesSettings[id]['lun'] = disk.lun;
      }
      if ('wwn' in disk) {
        this.imagesSettings[id]['wwn'] = disk.wwn;
      }

      this.onImageSelection({ option: { name: id, selected: true } });
    });
    this.targetForm.patchValue({
      disks: disks
    });

    _.forEach(res.clients, (client) => {
      const initiator = this.addInitiator();
      client.luns = _.map(client.luns, (lun) => `${lun.pool}/${lun.image}`);
      initiator.patchValue(client);
      // updatedInitiatorSelector()
    });

    _.forEach(res.groups, (group) => {
      const fg = this.addGroup();
      group.disks = _.map(group.disks, (disk) => `${disk.pool}/${disk.image}`);
      fg.patchValue(group);
      _.forEach(group.members, (member) => {
        this.onGroupMemberSelection({ option: new SelectOption(true, member, '') });
      });
    });
  }

  hasAdvancedSettings(settings: any) {
    return Object.values(settings).length > 0;
  }

  // Portals
  get portals() {
    return this.targetForm.get('portals') as FormControl;
  }

  onPortalSelection() {
    this.portals.setValue(this.portals.value);
  }

  removePortal(index: number, portal: string) {
    this.portalsSelections.forEach((value) => {
      if (value.name === portal) {
        value.selected = false;
      }
    });

    this.portals.value.splice(index, 1);
    this.portals.setValue(this.portals.value);
    return false;
  }

  // Images
  get disks() {
    return this.targetForm.get('disks') as FormControl;
  }

  removeImage(index: number, image: string) {
    this.imagesSelections.forEach((value) => {
      if (value.name === image) {
        value.selected = false;
      }
    });
    this.disks.value.splice(index, 1);
    this.removeImageRefs(image);
    this.targetForm.get('disks').updateValueAndValidity({ emitEvent: false });
    return false;
  }

  removeImageRefs(name: string) {
    this.initiators.controls.forEach((element) => {
      const newImages = element.value.luns.filter((item: string) => item !== name);
      element.get('luns').setValue(newImages);
    });

    this.groups.controls.forEach((element) => {
      const newDisks = element.value.disks.filter((item: string) => item !== name);
      element.get('disks').setValue(newDisks);
    });

    _.forEach(this.imagesInitiatorSelections, (selections, i) => {
      this.imagesInitiatorSelections[i] = selections.filter((item: any) => item.name !== name);
    });
    _.forEach(this.groupDiskSelections, (selections, i) => {
      this.groupDiskSelections[i] = selections.filter((item: any) => item.name !== name);
    });
  }

  getDefaultBackstore(imageId: string) {
    let result = this.default_backstore;
    const image = this.getImageById(imageId);
    if (!this.validFeatures(image, this.default_backstore)) {
      this.backstores.forEach((backstore) => {
        if (backstore !== this.default_backstore) {
          if (this.validFeatures(image, backstore)) {
            result = backstore;
          }
        }
      });
    }
    return result;
  }

  isLunIdInUse(lunId: string, imageId: string) {
    const images = this.disks.value.filter((currentImageId: string) => currentImageId !== imageId);
    return this.getLunIds(images).includes(lunId);
  }

  getLunIds(images: object) {
    return _.map(images, (image) => this.imagesSettings[image]['lun']);
  }

  nextLunId(imageId: string) {
    const images = this.disks.value.filter((currentImageId: string) => currentImageId !== imageId);
    const lunIdsInUse = this.getLunIds(images);
    let lunIdCandidate = 0;
    while (lunIdsInUse.includes(lunIdCandidate)) {
      lunIdCandidate++;
    }
    return lunIdCandidate;
  }

  getWwns(images: object) {
    const wwns = _.map(images, (image) => this.imagesSettings[image]['wwn']);
    return wwns.filter((wwn) => _.isString(wwn) && wwn !== '');
  }

  onImageSelection($event: any) {
    const option = $event.option;

    if (option.selected) {
      if (!this.imagesSettings[option.name]) {
        const defaultBackstore = this.getDefaultBackstore(option.name);
        this.imagesSettings[option.name] = {
          backstore: defaultBackstore,
          lun: this.nextLunId(option.name)
        };
        this.imagesSettings[option.name][defaultBackstore] = {};
      } else if (this.isLunIdInUse(this.imagesSettings[option.name]['lun'], option.name)) {
        // If the lun id is now in use, we have to generate a new one
        this.imagesSettings[option.name]['lun'] = this.nextLunId(option.name);
      }

      _.forEach(this.imagesInitiatorSelections, (selections, i) => {
        selections.push(new SelectOption(false, option.name, ''));
        this.imagesInitiatorSelections[i] = [...selections];
      });

      _.forEach(this.groupDiskSelections, (selections, i) => {
        selections.push(new SelectOption(false, option.name, ''));
        this.groupDiskSelections[i] = [...selections];
      });
    } else {
      this.removeImageRefs(option.name);
    }
    this.targetForm.get('disks').updateValueAndValidity({ emitEvent: false });
  }

  // Initiators
  get initiators() {
    return this.targetForm.get('initiators') as FormArray;
  }

  addInitiator() {
    const fg = new CdFormGroup({
      client_iqn: new FormControl('', {
        validators: [
          Validators.required,
          CdValidators.custom('notUnique', (client_iqn: string) => {
            const flattened = this.initiators.controls.reduce(function (accumulator, currentValue) {
              return accumulator.concat(currentValue.value.client_iqn);
            }, []);

            return flattened.indexOf(client_iqn) !== flattened.lastIndexOf(client_iqn);
          }),
          Validators.pattern(this.IQN_REGEX)
        ]
      }),
      auth: new CdFormGroup({
        user: new FormControl(''),
        password: new FormControl(''),
        mutual_user: new FormControl(''),
        mutual_password: new FormControl('')
      }),
      luns: new FormControl([]),
      cdIsInGroup: new FormControl(false)
    });

    this.setAuthValidator(fg);

    this.initiators.push(fg);

    _.forEach(this.groupMembersSelections, (selections, i) => {
      selections.push(new SelectOption(false, '', ''));
      this.groupMembersSelections[i] = [...selections];
    });

    const disks = _.map(
      this.targetForm.getValue('disks'),
      (disk) => new SelectOption(false, disk, '')
    );
    this.imagesInitiatorSelections.push(disks);

    return fg;
  }

  setAuthValidator(fg: CdFormGroup) {
    CdValidators.validateIf(
      fg.get('user'),
      () => fg.getValue('password') || fg.getValue('mutual_user') || fg.getValue('mutual_password'),
      [Validators.required],
      [Validators.pattern(this.USER_REGEX)],
      [fg.get('password'), fg.get('mutual_user'), fg.get('mutual_password')]
    );

    CdValidators.validateIf(
      fg.get('password'),
      () => fg.getValue('user') || fg.getValue('mutual_user') || fg.getValue('mutual_password'),
      [Validators.required],
      [Validators.pattern(this.PASSWORD_REGEX)],
      [fg.get('user'), fg.get('mutual_user'), fg.get('mutual_password')]
    );

    CdValidators.validateIf(
      fg.get('mutual_user'),
      () => fg.getValue('mutual_password'),
      [Validators.required],
      [Validators.pattern(this.USER_REGEX)],
      [fg.get('user'), fg.get('password'), fg.get('mutual_password')]
    );

    CdValidators.validateIf(
      fg.get('mutual_password'),
      () => fg.getValue('mutual_user'),
      [Validators.required],
      [Validators.pattern(this.PASSWORD_REGEX)],
      [fg.get('user'), fg.get('password'), fg.get('mutual_user')]
    );
  }

  removeInitiator(index: number) {
    const removed = this.initiators.value[index];

    this.initiators.removeAt(index);

    _.forEach(this.groupMembersSelections, (selections, i) => {
      selections.splice(index, 1);
      this.groupMembersSelections[i] = [...selections];
    });

    this.groups.controls.forEach((element) => {
      const newMembers = element.value.members.filter(
        (item: string) => item !== removed.client_iqn
      );
      element.get('members').setValue(newMembers);
    });

    this.imagesInitiatorSelections.splice(index, 1);
  }

  updatedInitiatorSelector() {
    // Validate all client_iqn
    this.initiators.controls.forEach((control) => {
      control.get('client_iqn').updateValueAndValidity({ emitEvent: false });
    });

    // Update Group Initiator Selector
    _.forEach(this.groupMembersSelections, (group, group_index) => {
      _.forEach(group, (elem, index) => {
        const oldName = elem.name;
        elem.name = this.initiators.controls[index].value.client_iqn;

        this.groups.controls.forEach((element) => {
          const members = element.value.members;
          const i = members.indexOf(oldName);

          if (i !== -1) {
            members[i] = elem.name;
          }
          element.get('members').setValue(members);
        });
      });
      this.groupMembersSelections[group_index] = [...this.groupMembersSelections[group_index]];
    });
  }

  removeInitiatorImage(initiator: any, lun_index: number, initiator_index: number, image: string) {
    const luns = initiator.getValue('luns');
    luns.splice(lun_index, 1);
    initiator.patchValue({ luns: luns });

    this.imagesInitiatorSelections[initiator_index].forEach((value: Record<string, any>) => {
      if (value.name === image) {
        value.selected = false;
      }
    });

    return false;
  }

  // Groups
  get groups() {
    return this.targetForm.get('groups') as FormArray;
  }

  addGroup() {
    const fg = new CdFormGroup({
      group_id: new FormControl('', { validators: [Validators.required] }),
      members: new FormControl([]),
      disks: new FormControl([])
    });

    this.groups.push(fg);

    const disks = _.map(
      this.targetForm.getValue('disks'),
      (disk) => new SelectOption(false, disk, '')
    );
    this.groupDiskSelections.push(disks);

    const initiators = _.map(
      this.initiators.value,
      (initiator) => new SelectOption(false, initiator.client_iqn, '', !initiator.cdIsInGroup)
    );
    this.groupMembersSelections.push(initiators);

    return fg;
  }

  removeGroup(index: number) {
    this.groups.removeAt(index);
    this.groupDiskSelections.splice(index, 1);
  }

  onGroupMemberSelection($event: any) {
    const option = $event.option;

    let initiator_index: number;
    this.initiators.controls.forEach((element, index) => {
      if (element.value.client_iqn === option.name) {
        element.patchValue({ luns: [] });
        element.get('cdIsInGroup').setValue(option.selected);
        initiator_index = index;
      }
    });

    // Members can only be at one group at a time, so when a member is selected
    // in one group we need to disable its selection in other groups
    _.forEach(this.groupMembersSelections, (group) => {
      group[initiator_index].enabled = !option.selected;
    });
  }

  removeGroupInitiator(group: CdFormGroup, member_index: number, group_index: number) {
    const name = group.getValue('members')[member_index];
    group.getValue('members').splice(member_index, 1);

    this.groupMembersSelections[group_index].forEach((value) => {
      if (value.name === name) {
        value.selected = false;
      }
    });
    this.groupMembersSelections[group_index] = [...this.groupMembersSelections[group_index]];

    this.onGroupMemberSelection({ option: new SelectOption(false, name, '') });
  }

  removeGroupDisk(group: CdFormGroup, disk_index: number, group_index: number) {
    const name = group.getValue('disks')[disk_index];
    group.getValue('disks').splice(disk_index, 1);

    this.groupDiskSelections[group_index].forEach((value) => {
      if (value.name === name) {
        value.selected = false;
      }
    });
    this.groupDiskSelections[group_index] = [...this.groupDiskSelections[group_index]];
  }

  submit() {
    const formValue = _.cloneDeep(this.targetForm.value);

    const request: Record<string, any> = {
      target_iqn: this.targetForm.getValue('target_iqn'),
      target_controls: this.targetForm.getValue('target_controls'),
      acl_enabled: this.targetForm.getValue('acl_enabled'),
      portals: [],
      disks: [],
      clients: [],
      groups: []
    };

    // Target level authentication was introduced in ceph-iscsi config v11
    if (this.cephIscsiConfigVersion > 10) {
      const targetAuth: CdFormGroup = this.targetForm.get('auth') as CdFormGroup;
      if (!targetAuth.getValue('user')) {
        targetAuth.get('user').setValue('');
      }
      if (!targetAuth.getValue('password')) {
        targetAuth.get('password').setValue('');
      }
      if (!targetAuth.getValue('mutual_user')) {
        targetAuth.get('mutual_user').setValue('');
      }
      if (!targetAuth.getValue('mutual_password')) {
        targetAuth.get('mutual_password').setValue('');
      }
      const acl_enabled = this.targetForm.getValue('acl_enabled');
      request['auth'] = {
        user: acl_enabled ? '' : targetAuth.getValue('user'),
        password: acl_enabled ? '' : targetAuth.getValue('password'),
        mutual_user: acl_enabled ? '' : targetAuth.getValue('mutual_user'),
        mutual_password: acl_enabled ? '' : targetAuth.getValue('mutual_password')
      };
    }

    // Disks
    formValue.disks.forEach((disk: string) => {
      const imageSplit = disk.split('/');
      const backstore = this.imagesSettings[disk].backstore;
      request.disks.push({
        pool: imageSplit[0],
        image: imageSplit[1],
        backstore: backstore,
        controls: this.imagesSettings[disk][backstore],
        lun: this.imagesSettings[disk]['lun'],
        wwn: this.imagesSettings[disk]['wwn']
      });
    });

    // Portals
    formValue.portals.forEach((portal: string) => {
      const index = portal.indexOf(':');
      request.portals.push({
        host: portal.substring(0, index),
        ip: portal.substring(index + 1)
      });
    });

    // Clients
    if (request.acl_enabled) {
      formValue.initiators.forEach((initiator: Record<string, any>) => {
        if (!initiator.auth.user) {
          initiator.auth.user = '';
        }
        if (!initiator.auth.password) {
          initiator.auth.password = '';
        }
        if (!initiator.auth.mutual_user) {
          initiator.auth.mutual_user = '';
        }
        if (!initiator.auth.mutual_password) {
          initiator.auth.mutual_password = '';
        }
        delete initiator.cdIsInGroup;

        const newLuns: any[] = [];
        initiator.luns.forEach((lun: string) => {
          const imageSplit = lun.split('/');
          newLuns.push({
            pool: imageSplit[0],
            image: imageSplit[1]
          });
        });

        initiator.luns = newLuns;
      });
      request.clients = formValue.initiators;
    }

    // Groups
    if (request.acl_enabled) {
      formValue.groups.forEach((group: Record<string, any>) => {
        const newDisks: any[] = [];
        group.disks.forEach((disk: string) => {
          const imageSplit = disk.split('/');
          newDisks.push({
            pool: imageSplit[0],
            image: imageSplit[1]
          });
        });

        group.disks = newDisks;
      });
      request.groups = formValue.groups;
    }

    let wrapTask;
    if (this.isEdit) {
      request['new_target_iqn'] = request.target_iqn;
      request.target_iqn = this.target_iqn;
      wrapTask = this.taskWrapper.wrapTaskAroundCall({
        task: new FinishedTask('iscsi/target/edit', {
          target_iqn: request.target_iqn
        }),
        call: this.iscsiService.updateTarget(this.target_iqn, request)
      });
    } else {
      wrapTask = this.taskWrapper.wrapTaskAroundCall({
        task: new FinishedTask('iscsi/target/create', {
          target_iqn: request.target_iqn
        }),
        call: this.iscsiService.createTarget(request)
      });
    }

    wrapTask.subscribe({
      error: () => {
        this.targetForm.setErrors({ cdSubmitButton: true });
      },
      complete: () => this.router.navigate(['/block/iscsi/targets'])
    });
  }

  targetSettingsModal() {
    const initialState = {
      target_controls: this.targetForm.get('target_controls'),
      target_default_controls: this.target_default_controls,
      target_controls_limits: this.target_controls_limits
    };

    this.modalRef = this.modalService.show(IscsiTargetIqnSettingsModalComponent, initialState);
  }

  imageSettingsModal(image: string) {
    const initialState = {
      imagesSettings: this.imagesSettings,
      image: image,
      api_version: this.api_version,
      disk_default_controls: this.disk_default_controls,
      disk_controls_limits: this.disk_controls_limits,
      backstores: this.getValidBackstores(this.getImageById(image)),
      control: this.targetForm.get('disks')
    };

    this.modalRef = this.modalService.show(IscsiTargetImageSettingsModalComponent, initialState);
  }

  validFeatures(image: Record<string, any>, backstore: string) {
    const imageFeatures = image.features;
    const requiredFeatures = this.required_rbd_features[backstore];
    const unsupportedFeatures = this.unsupported_rbd_features[backstore];
    // tslint:disable-next-line:no-bitwise
    const validRequiredFeatures = (imageFeatures & requiredFeatures) === requiredFeatures;
    // tslint:disable-next-line:no-bitwise
    const validSupportedFeatures = (imageFeatures & unsupportedFeatures) === 0;
    return validRequiredFeatures && validSupportedFeatures;
  }

  getImageById(imageId: string) {
    return this.imagesAll.find((image) => imageId === `${image.pool_name}/${image.name}`);
  }

  getValidBackstores(image: object) {
    return this.backstores.filter((backstore) => this.validFeatures(image, backstore));
  }
}
