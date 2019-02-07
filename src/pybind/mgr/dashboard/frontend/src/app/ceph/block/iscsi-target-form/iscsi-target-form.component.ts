import { Component, OnInit } from '@angular/core';
import { FormArray, FormControl, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';
import { BsModalRef, BsModalService } from 'ngx-bootstrap/modal';
import { forkJoin } from 'rxjs';

import { IscsiService } from '../../../shared/api/iscsi.service';
import { RbdService } from '../../../shared/api/rbd.service';
import { SelectMessages } from '../../../shared/components/select/select-messages.model';
import { SelectOption } from '../../../shared/components/select/select-option.model';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';
import { CdValidators } from '../../../shared/forms/cd-validators';
import { FinishedTask } from '../../../shared/models/finished-task';
import { TaskWrapperService } from '../../../shared/services/task-wrapper.service';
import { IscsiTargetImageSettingsModalComponent } from '../iscsi-target-image-settings-modal/iscsi-target-image-settings-modal.component';
import { IscsiTargetIqnSettingsModalComponent } from '../iscsi-target-iqn-settings-modal/iscsi-target-iqn-settings-modal.component';

@Component({
  selector: 'cd-iscsi-target-form',
  templateUrl: './iscsi-target-form.component.html',
  styleUrls: ['./iscsi-target-form.component.scss']
})
export class IscsiTargetFormComponent implements OnInit {
  targetForm: CdFormGroup;
  modalRef: BsModalRef;
  minimum_gateways = 1;
  target_default_controls: any;
  disk_default_controls: any;

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
    portals: new SelectMessages(
      { noOptions: this.i18n('There are no portals available.') },
      this.i18n
    ),
    images: new SelectMessages(
      { noOptions: this.i18n('There are no images available.') },
      this.i18n
    ),
    initiatorImage: new SelectMessages(
      {
        noOptions: this.i18n(
          'There are no images available. Please make sure you add an image to the target.'
        )
      },
      this.i18n
    ),
    groupInitiator: new SelectMessages(
      {
        noOptions: this.i18n(
          'There are no initiators available. Please make sure you add an initiator to the target.'
        )
      },
      this.i18n
    )
  };

  IQN_REGEX = /^iqn\.(19|20)\d\d-(0[1-9]|1[0-2])\.\D{2,3}(\.[A-Za-z0-9-]+)+(:[A-Za-z0-9-\.]+)*$/;
  USER_REGEX = /[\w\.:@_-]{8,64}/;
  PASSWORD_REGEX = /[\w@\-_]{12,16}/;

  constructor(
    private iscsiService: IscsiService,
    private modalService: BsModalService,
    private rbdService: RbdService,
    private router: Router,
    private route: ActivatedRoute,
    private i18n: I18n,
    private taskWrapper: TaskWrapperService
  ) {}

  ngOnInit() {
    const promises: any[] = [
      this.iscsiService.listTargets(),
      this.rbdService.list(),
      this.iscsiService.portals(),
      this.iscsiService.settings()
    ];

    if (this.router.url.startsWith('/block/iscsi/targets/edit')) {
      this.isEdit = true;
      this.route.params.subscribe((params: { target_iqn: string }) => {
        this.target_iqn = decodeURIComponent(params.target_iqn);
        promises.push(this.iscsiService.getTarget(this.target_iqn));
      });
    }

    forkJoin(promises).subscribe((data: any[]) => {
      // iscsiService.listTargets
      const usedImages = _(data[0])
        .filter((target) => target.target_iqn !== this.target_iqn)
        .flatMap((target) => target.disks)
        .map((image) => `${image.pool}/${image.image}`)
        .value();

      // rbdService.list()
      this.imagesAll = _(data[1])
        .flatMap((pool) => pool.value)
        .map((image) => `${image.pool_name}/${image.name}`)
        .filter((image) => usedImages.indexOf(image) === -1)
        .value();

      this.imagesSelections = this.imagesAll.map((image) => new SelectOption(false, image, ''));

      // iscsiService.portals()
      const portals: SelectOption[] = [];
      data[2].forEach((portal) => {
        portal.ip_addresses.forEach((ip) => {
          portals.push(new SelectOption(false, portal.name + ':' + ip, ''));
        });
      });
      this.portalsSelections = [...portals];

      // iscsiService.settings()
      this.minimum_gateways = data[3].config.minimum_gateways;
      this.target_default_controls = data[3].target_default_controls;
      this.disk_default_controls = data[3].disk_default_controls;

      this.createForm();

      // iscsiService.getTarget()
      if (data[4]) {
        this.resolveModel(data[4]);
      }
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
          CdValidators.custom('minGateways', (value) => {
            const gateways = _.uniq(value.map((elem) => elem.split(':')[0]));
            return gateways.length < Math.max(1, this.minimum_gateways);
          })
        ]
      }),
      disks: new FormControl([]),
      initiators: new FormArray([]),
      groups: new FormArray([])
    });
  }

  resolveModel(res) {
    this.targetForm.patchValue({
      target_iqn: res.target_iqn,
      target_controls: res.target_controls
    });

    const portals = [];
    _.forEach(res.portals, (portal) => {
      const id = `${portal.host}:${portal.ip}`;
      portals.push(id);
    });
    this.targetForm.patchValue({
      portals: portals
    });

    const disks = [];
    _.forEach(res.disks, (disk) => {
      const id = `${disk.pool}/${disk.image}`;
      disks.push(id);
      this.imagesSettings[id] = disk.controls;
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
      console.log(group);
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
    return false;
  }

  removeImageRefs(name) {
    this.initiators.controls.forEach((element) => {
      const newImages = element.value.luns.filter((item) => item !== name);
      element.get('luns').setValue(newImages);
    });

    this.groups.controls.forEach((element) => {
      const newDisks = element.value.disks.filter((item) => item !== name);
      element.get('disks').setValue(newDisks);
    });

    _.forEach(this.imagesInitiatorSelections, (selections, i) => {
      this.imagesInitiatorSelections[i] = selections.filter((item: any) => item.name !== name);
    });
    _.forEach(this.groupDiskSelections, (selections, i) => {
      this.groupDiskSelections[i] = selections.filter((item: any) => item.name !== name);
    });
  }

  onImageSelection($event) {
    const option = $event.option;

    if (option.selected) {
      if (!this.imagesSettings[option.name]) {
        this.imagesSettings[option.name] = {};
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
          CdValidators.custom('notUnique', (client_iqn) => {
            const flattened = this.initiators.controls.reduce(function(accumulator, currentValue) {
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

  removeInitiator(index) {
    const removed = this.initiators.value[index];

    this.initiators.removeAt(index);

    _.forEach(this.groupMembersSelections, (selections, i) => {
      selections.splice(index, 1);
      this.groupMembersSelections[i] = [...selections];
    });

    this.groups.controls.forEach((element) => {
      const newMembers = element.value.members.filter((item) => item !== removed.client_iqn);
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

  removeInitiatorImage(initiator: any, lun_index: number, initiator_index: string, image: string) {
    const luns = initiator.getValue('luns');
    luns.splice(lun_index, 1);
    initiator.patchValue({ luns: luns });

    this.imagesInitiatorSelections[initiator_index].forEach((value) => {
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
      (initiator) => new SelectOption(false, initiator.client_iqn, '')
    );
    this.groupMembersSelections.push(initiators);

    return fg;
  }

  removeGroup(index) {
    this.groups.removeAt(index);
    this.groupDiskSelections.splice(index, 1);
  }

  onGroupMemberSelection($event) {
    const option = $event.option;

    this.initiators.controls.forEach((element) => {
      if (element.value.client_iqn === option.name) {
        element.patchValue({ luns: [] });
        element.get('cdIsInGroup').setValue(option.selected);
      }
    });
  }

  removeGroupInitiator(group, member_index, group_index) {
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

  removeGroupDisk(group, disk_index, group_index) {
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
    const formValue = this.targetForm.value;

    const request = {
      target_iqn: this.targetForm.getValue('target_iqn'),
      target_controls: this.targetForm.getValue('target_controls'),
      portals: [],
      disks: [],
      clients: [],
      groups: []
    };

    // Disks
    formValue.disks.forEach((disk) => {
      const imageSplit = disk.split('/');
      request.disks.push({
        pool: imageSplit[0],
        image: imageSplit[1],
        controls: this.imagesSettings[disk]
      });
    });

    // Portals
    formValue.portals.forEach((portal) => {
      const portalSplit = portal.split(':');
      request.portals.push({
        host: portalSplit[0],
        ip: portalSplit[1]
      });
    });

    // Clients
    formValue.initiators.forEach((initiator) => {
      if (!initiator.auth.user) {
        initiator.auth.user = null;
      }
      if (!initiator.auth.password) {
        initiator.auth.password = null;
      }
      if (!initiator.auth.mutual_user) {
        initiator.auth.mutual_user = null;
      }
      if (!initiator.auth.mutual_password) {
        initiator.auth.mutual_password = null;
      }

      const newLuns = [];
      initiator.luns.forEach((lun) => {
        const imageSplit = lun.split('/');
        newLuns.push({
          pool: imageSplit[0],
          image: imageSplit[1]
        });
      });

      initiator.luns = newLuns;
    });
    request.clients = formValue.initiators;

    // Groups
    formValue.groups.forEach((group) => {
      const newDisks = [];
      group.disks.forEach((disk) => {
        const imageSplit = disk.split('/');
        newDisks.push({
          pool: imageSplit[0],
          image: imageSplit[1]
        });
      });

      group.disks = newDisks;
    });
    request.groups = formValue.groups;

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

    wrapTask.subscribe(
      undefined,
      () => {
        this.targetForm.setErrors({ cdSubmitButton: true });
      },
      () => this.router.navigate(['/block/iscsi/targets'])
    );
  }

  targetSettingsModal() {
    const initialState = {
      target_controls: this.targetForm.get('target_controls'),
      target_default_controls: this.target_default_controls
    };

    this.modalRef = this.modalService.show(IscsiTargetIqnSettingsModalComponent, { initialState });
  }

  imageSettingsModal(image) {
    const initialState = {
      imagesSettings: this.imagesSettings,
      image: image,
      disk_default_controls: this.disk_default_controls
    };

    this.modalRef = this.modalService.show(IscsiTargetImageSettingsModalComponent, {
      initialState
    });
  }
}
