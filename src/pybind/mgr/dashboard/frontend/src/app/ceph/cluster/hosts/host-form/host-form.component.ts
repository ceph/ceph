import { Component, OnInit } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';
import { Router } from '@angular/router';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import expand from 'brace-expansion';

import { HostService } from '~/app/shared/api/host.service';
import { SelectMessages } from '~/app/shared/components/select/select-messages.model';
import { SelectOption } from '~/app/shared/components/select/select-option.model';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';

@Component({
  selector: 'cd-host-form',
  templateUrl: './host-form.component.html',
  styleUrls: ['./host-form.component.scss']
})
export class HostFormComponent extends CdForm implements OnInit {
  hostForm: CdFormGroup;
  action: string;
  resource: string;
  hostnames: string[];
  hostnameArray: string[] = [];
  addr: string;
  status: string;
  allLabels: string[];
  pageURL: string;
  hostPattern = false;
  labelsOption: Array<SelectOption> = [];
  hideMaintenance: boolean;

  messages = new SelectMessages({
    empty: $localize`There are no labels.`,
    filter: $localize`Filter or add labels`,
    add: $localize`Add label`
  });

  constructor(
    private router: Router,
    private actionLabels: ActionLabelsI18n,
    private hostService: HostService,
    private taskWrapper: TaskWrapperService,
    public activeModal: NgbActiveModal
  ) {
    super();
    this.resource = $localize`host`;
    this.action = this.actionLabels.ADD;
  }

  ngOnInit() {
    if (this.router.url.includes('hosts')) {
      this.pageURL = 'hosts';
    }
    this.createForm();
    const hostContext = new CdTableFetchDataContext(() => undefined);
    this.hostService.list(hostContext.toParams(), 'false').subscribe((resp: any[]) => {
      this.hostnames = resp.map((host) => {
        return host['hostname'];
      });
      this.loadingReady();
    });

    this.hostService.getLabels().subscribe((resp: string[]) => {
      const uniqueLabels = new Set(resp.concat(this.hostService.predefinedLabels));
      this.labelsOption = Array.from(uniqueLabels).map((label) => {
        return { enabled: true, name: label, selected: false, description: null };
      });
    });
  }

  // check if hostname is a single value or pattern to hide network address field
  checkHostNameValue() {
    const hostNames = this.hostForm.get('hostname').value;
    hostNames.match(/[()\[\]{},]/g) ? (this.hostPattern = true) : (this.hostPattern = false);
  }

  private createForm() {
    this.hostForm = new CdFormGroup({
      hostname: new UntypedFormControl('', {
        validators: [
          Validators.required,
          CdValidators.custom('uniqueName', (hostname: string) => {
            return this.hostnames && this.hostnames.indexOf(hostname) !== -1;
          })
        ]
      }),
      addr: new UntypedFormControl('', {
        validators: [CdValidators.ip()]
      }),
      labels: new UntypedFormControl([]),
      maintenance: new UntypedFormControl(false)
    });
  }

  private isCommaSeparatedPattern(hostname: string) {
    // eg. ceph-node-01.cephlab.com,ceph-node-02.cephlab.com
    return hostname.includes(',');
  }

  private isRangeTypePattern(hostname: string) {
    // check if it is a range expression or comma separated range expression
    // eg. ceph-mon-[01-05].lab.com,ceph-osd-[02-08].lab.com,ceph-rgw-[01-09]
    return hostname.includes('[') && hostname.includes(']') && !hostname.match(/(?![^(]*\)),/g);
  }

  private replaceBraces(hostname: string) {
    // pattern to replace range [0-5] to [0..5](valid expression for brace expansion)
    // replace any kind of brackets with curly braces
    return hostname
      .replace(/(\d)\s*-\s*(\d)/g, '$1..$2')
      .replace(/\(/g, '{')
      .replace(/\)/g, '}')
      .replace(/\[/g, '{')
      .replace(/]/g, '}');
  }

  // expand hostnames in case hostname is a pattern
  private checkHostNamePattern(hostname: string) {
    if (this.isRangeTypePattern(hostname)) {
      const hostnameRange = this.replaceBraces(hostname);
      this.hostnameArray = expand(hostnameRange);
    } else if (this.isCommaSeparatedPattern(hostname)) {
      let hostArray = [];
      hostArray = hostname.split(',');
      hostArray.forEach((host: string) => {
        if (this.isRangeTypePattern(host)) {
          const hostnameRange = this.replaceBraces(host);
          this.hostnameArray = this.hostnameArray.concat(expand(hostnameRange));
        } else {
          this.hostnameArray.push(host);
        }
      });
    } else {
      // single hostname
      this.hostnameArray.push(hostname);
    }
  }

  submit() {
    const hostname = this.hostForm.get('hostname').value;
    this.checkHostNamePattern(hostname);
    this.addr = this.hostForm.get('addr').value;
    this.status = this.hostForm.get('maintenance').value ? 'maintenance' : '';
    this.allLabels = this.hostForm.get('labels').value;
    if (this.pageURL !== 'hosts' && !this.allLabels.includes('_no_schedule')) {
      this.allLabels.push('_no_schedule');
    }
    this.hostnameArray.forEach((hostName: string) => {
      this.taskWrapper
        .wrapTaskAroundCall({
          task: new FinishedTask('host/' + URLVerbs.ADD, {
            hostname: hostName
          }),
          call: this.hostService.create(hostName, this.addr, this.allLabels, this.status)
        })
        .subscribe({
          error: () => {
            this.hostForm.setErrors({ cdSubmitButton: true });
          },
          complete: () => {
            this.pageURL === 'hosts'
              ? this.router.navigate([this.pageURL, { outlets: { modal: null } }])
              : this.activeModal.close();
          }
        });
    });
  }
}
