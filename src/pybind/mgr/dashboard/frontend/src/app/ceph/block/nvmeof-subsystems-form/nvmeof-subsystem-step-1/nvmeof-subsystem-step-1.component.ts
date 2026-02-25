import { Component, Input, OnInit } from '@angular/core';
import { forkJoin, of } from 'rxjs';
import { UntypedFormControl, Validators } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';

import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { TearsheetStep } from '~/app/shared/models/tearsheet-step';
import { Host } from '~/app/shared/models/host.interface';
import { ListenerItem } from '~/app/shared/models/nvmeof';

@Component({
  selector: 'cd-nvmeof-subsystem-step-one',
  templateUrl: './nvmeof-subsystem-step-1.component.html',
  styleUrls: ['./nvmeof-subsystem-step-1.component.scss'],
  standalone: false
})
export class NvmeofSubsystemsStepOneComponent implements OnInit, TearsheetStep {
  @Input() group!: string;
  @Input() subsystemNQN: string;
  @Input() listenersOnly = false;
  formGroup: CdFormGroup;
  action: string;
  pageURL: string;
  INVALID_TEXTS = {
    required: $localize`This field is required`,
    nqnPattern: $localize`Expected NQN format is "nqn.$year-$month.$reverseDomainName:$utf8-string" or "nqn.2014-08.org.nvmexpress:uuid:$UUID-string"`,
    notUnique: $localize`This NQN is already in use`,
    maxLength: $localize`An NQN may not be more than 223 bytes in length.`
  };

  hosts: ListenerItem[] = [];

  constructor(
    public actionLabels: ActionLabelsI18n,
    public activeModal: NgbActiveModal,
    private nvmeofService: NvmeofService
  ) {}

  DEFAULT_NQN = 'nqn.2001-07.com.ceph:' + Date.now();
  NQN_REGEX = /^nqn\.(19|20)\d\d-(0[1-9]|1[0-2])\.\D{2,3}(\.[A-Za-z0-9-]+)+(:[A-Za-z0-9-\.]+(:[A-Za-z0-9-\.]+)*)$/;
  NQN_REGEX_UUID = /^nqn\.2014-08\.org\.nvmexpress:uuid:[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;

  customNQNValidator = CdValidators.custom(
    'nqnPattern',
    (nqnInput: string) =>
      !!nqnInput && !(this.NQN_REGEX.test(nqnInput) || this.NQN_REGEX_UUID.test(nqnInput))
  );

  ngOnInit() {
    this.createForm();
    this.setHosts();
  }

  setHosts() {
    const hosts$ = this.nvmeofService.getHostsForGroup(this.group);
    const listeners$ =
      this.listenersOnly && this.subsystemNQN
        ? this.nvmeofService.listListeners(this.subsystemNQN, this.group)
        : of(null);

    forkJoin([hosts$, listeners$]).subscribe(
      ([nvmeofGwNodes, existingListeners]: [Host[], any]) => {
        const listeners = Array.isArray(existingListeners)
          ? existingListeners
          : existingListeners?.listeners || [];
        const consumedHosts = new Set(listeners.map((l: any) => l.host_name));
        this.hosts = nvmeofGwNodes
          .map((h) => ({ content: h.hostname, addr: h.addr }))
          .filter((h) => !consumedHosts.has(h.content));
      }
    );
  }

  createForm() {
    if (this.listenersOnly) {
      this.formGroup = new CdFormGroup({
        listeners: new UntypedFormControl([])
      });
    } else {
      this.formGroup = new CdFormGroup({
        nqn: new UntypedFormControl(this.DEFAULT_NQN, {
          validators: [
            this.customNQNValidator,
            Validators.required,
            CdValidators.custom(
              'maxLength',
              (nqnInput: string) => new TextEncoder().encode(nqnInput).length > 223
            )
          ],
          asyncValidators: [
            CdValidators.unique(
              this.nvmeofService.isSubsystemPresent,
              this.nvmeofService,
              null,
              null,
              this.group
            )
          ]
        }),
        listeners: new UntypedFormControl([])
      });
    }
  }

  removeListener(index: number) {
    const listeners = this.formGroup.get('listeners').value;
    listeners.splice(index, 1);
    this.formGroup.get('listeners').setValue([...listeners]);
  }
}
