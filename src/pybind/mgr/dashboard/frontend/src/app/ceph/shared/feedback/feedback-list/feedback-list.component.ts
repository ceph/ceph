import { Component, OnInit, ViewChild } from '@angular/core';
import { BehaviorSubject, Observable } from 'rxjs';
import { switchMap } from 'rxjs/operators';
import { FeedbackService } from '~/app/shared/api/feedback.service';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';
import { FeedbackComponent } from '../feedback.component';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';

@Component({
  selector: 'cd-feedback-form',
  templateUrl: './feedback-list.component.html',
  styleUrls: ['./feedback-list.component.scss']
})
export class FeedbackListComponent implements OnInit {
  @ViewChild('subjectTpl', { static: true })
  subjectTpl: any;

  permissions: Permissions;
  columns: CdTableColumn[] = [];
  tableActions: CdTableAction[];
  selection = new CdTableSelection();

  issues$: Observable<any>;
  subject = new BehaviorSubject<any>([]);

  icons = Icons;

  constructor(
    private feedbackService: FeedbackService,
    private authStorageService: AuthStorageService,
    private modalService: ModalService,
    private actionLabels: ActionLabelsI18n
  ) {
    this.permissions = this.authStorageService.getPermissions();
  }

  ngOnInit() {
    this.columns = [
      {
        name: $localize`Subject`,
        prop: 'subject',
        flexGrow: 1.5,
        cellTemplate: this.subjectTpl
      },
      {
        name: $localize`Status`,
        prop: 'status.name',
        flexGrow: 0.5
      },
      {
        name: $localize`Type`,
        prop: 'tracker.name',
        flexGrow: 0.5,
        cellTransformation: CellTemplate.badge,
        customTemplateConfig: {
          map: {
            Bug: { class: 'badge-danger' },
            Feature: { class: 'badge-success' },
            Cleanup: { class: 'badge-warning' }
          }
        }
      },
      {
        name: $localize`Project`,
        prop: 'project.name',
        flexGrow: 0.5
      },
      {
        name: $localize`Created`,
        prop: 'created_on',
        flexGrow: 0.5,
        cellTransformation: CellTemplate.timeAgo
      }
    ];

    this.tableActions = [
      {
        name: this.actionLabels.CREATE,
        permission: 'create',
        icon: Icons.add,
        click: () => this.openModal()
      }
    ];

    this.issues$ = this.subject.pipe(
      switchMap(() => this.feedbackService.list())
    )
    this.subject.next([]);
  }

  openModal() {
    this.modalService.show(
      FeedbackComponent,
      null,
      {
        size: 'lg'
      }
    );
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }
}
