import { Component, EventEmitter, Input, OnInit, Output } from '@angular/core';
import { I18n } from '@ngx-translate/i18n-polyfill';

import * as _ from 'lodash';
import { BsModalRef } from 'ngx-bootstrap/modal';

import { OsdService } from '../../../../shared/api/osd.service';
import { ListWithDetails } from '../../../../shared/classes/list-with-details.class';
import { ActionLabelsI18n, URLVerbs } from '../../../../shared/constants/app.constants';
import { CellTemplate } from '../../../../shared/enum/cell-template.enum';
import { CdFormBuilder } from '../../../../shared/forms/cd-form-builder';
import { CdFormGroup } from '../../../../shared/forms/cd-form-group';
import { CdTableColumn } from '../../../../shared/models/cd-table-column';
import { CdTableFetchDataContext } from '../../../../shared/models/cd-table-fetch-data-context';
import { FinishedTask } from '../../../../shared/models/finished-task';
import { OSDPreview } from '../../../../shared/models/osd-preview.interface';
import { FormatterService } from '../../../../shared/services/formatter.service';
import { TaskWrapperService } from '../../../../shared/services/task-wrapper.service';

@Component({
  selector: 'cd-osd-creation-preview-modal',
  templateUrl: './osd-creation-preview-modal.component.html',
  styleUrls: ['./osd-creation-preview-modal.component.scss']
})
export class OsdCreationPreviewModalComponent extends ListWithDetails implements OnInit {
  @Input()
  driveGroups: Object[] = [];

  @Output()
  submitAction = new EventEmitter();

  action: string;
  formGroup: CdFormGroup;

  columns: Array<CdTableColumn> = [];
  osds: OSDPreview[] = [];
  updating = false;
  capacity = 0;

  constructor(
    public bsModalRef: BsModalRef,
    public actionLabels: ActionLabelsI18n,
    private formBuilder: CdFormBuilder,
    private osdService: OsdService,
    private taskWrapper: TaskWrapperService,
    private i18n: I18n,
    private formatterService: FormatterService
  ) {
    super();
    this.action = actionLabels.CREATE;
    this.createForm();
  }

  ngOnInit() {
    this.columns = [
      {
        name: this.i18n('Hostname'),
        prop: 'hostname',
        filterable: true
      },
      {
        name: this.i18n('Primary devices'),
        prop: 'data.devices_names',
        cellTransformation: CellTemplate.badge,
        customTemplateConfig: {
          class: 'badge-hdd'
        }
      },
      {
        name: this.i18n('Size'),
        prop: 'data.human_readable_size'
      },
      {
        name: this.i18n('WAL devices'),
        prop: 'wal.vg.devices_names',
        cellTransformation: CellTemplate.badge,
        customTemplateConfig: {
          class: 'badge-ssd'
        }
      },
      {
        name: this.i18n('WAL size'),
        prop: 'wal.human_readable_size'
      },
      {
        name: this.i18n('DB devices'),
        prop: 'db.vg.devices_names',
        cellTransformation: CellTemplate.badge,
        customTemplateConfig: {
          class: 'badge-ssd'
        }
      },
      {
        name: this.i18n('DB size'),
        prop: 'db.human_readable_size'
      }
    ];
  }

  createForm() {
    this.formGroup = this.formBuilder.group({});
  }

  private updateCapacity() {
    // Data returned from orchestrator are inconsitent between OSDs with and without shared devices.
    // Workaround this by convert string like "10 GB" back to 10 * 1024^3
    this.capacity = _.sumBy(this.osds, (osd) => {
      return this.formatterService.toBytes(osd.data.human_readable_size);
    });
  }

  private preparePreviews(previews: OSDPreview[]): OSDPreview[] {
    let i = 0;
    return previews.map((osd) => {
      osd.id = i;
      i += 1;
      osd.data.devices_names = _.map(osd.data.devices, 'path');
      if (!_.isEmpty(osd.wal)) {
        osd.wal.vg.devices_names = _.map(osd.wal.vg.devices, 'path');
      }
      if (!_.isEmpty(osd.db)) {
        osd.db.vg.devices_names = _.map(osd.db.vg.devices, 'path');
      }
      return osd;
    });
  }

  getPreview(context: CdTableFetchDataContext) {
    if (this.updating) {
      return;
    }
    this.updating = true;
    this.osdService.preview(this.driveGroups).subscribe(
      (resp) => {
        this.osds = this.preparePreviews(resp);
        this.updateCapacity();
        this.updating = false;
      },
      () => {
        context.error();
        this.updating = false;
      }
    );
  }

  onSubmit() {
    this.taskWrapper
      .wrapTaskAroundCall({
        task: new FinishedTask('osd/' + URLVerbs.CREATE, {
          tracking_id: _.join(_.map(this.driveGroups, 'service_id'), ', ')
        }),
        call: this.osdService.create(this.driveGroups)
      })
      .subscribe(
        undefined,
        () => {
          this.formGroup.setErrors({ cdSubmitButton: true });
        },
        () => {
          this.submitAction.emit();
          this.bsModalRef.hide();
        }
      );
  }
}
