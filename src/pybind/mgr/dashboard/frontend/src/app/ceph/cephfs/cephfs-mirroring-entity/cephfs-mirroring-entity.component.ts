import {
  Component,
  OnInit,
  OnChanges,
  SimpleChanges,
  Output,
  EventEmitter,
  Input,
  inject,
  ViewChild
} from '@angular/core';
import { BehaviorSubject, Observable, of } from 'rxjs';
import { catchError, map, switchMap, defaultIfEmpty } from 'rxjs/operators';

import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CephfsService } from '~/app/shared/api/cephfs.service';
import { ClusterService } from '~/app/shared/api/cluster.service';

import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { Validators, AbstractControl, ValidationErrors, ValidatorFn } from '@angular/forms';
import { CdForm } from '~/app/shared/forms/cd-form';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { FilesystemRow, MirroringEntityRow } from '~/app/shared/models/cephfs.model';
import { CephAuthUser } from '~/app/shared/models/cluster.model';

@Component({
  selector: 'cd-cephfs-mirroring-entity',
  templateUrl: './cephfs-mirroring-entity.component.html',
  styleUrls: ['./cephfs-mirroring-entity.component.scss'],
  standalone: false
})
export class CephfsMirroringEntityComponent extends CdForm implements OnInit, OnChanges {
  @ViewChild('table') table: TableComponent;
  columns: CdTableColumn[];
  selection = new CdTableSelection();

  subject$ = new BehaviorSubject<void>(undefined);
  entities$: Observable<MirroringEntityRow[]>;
  context: CdTableFetchDataContext;
  capabilities = [
    { name: $localize`MDS`, permission: 'rwps' },
    { name: $localize`MON`, permission: 'rwps' },
    { name: $localize`OSD`, permission: 'rwps' }
  ];

  isCreatingNewEntity = true;
  showCreateRequirementsWarning = true;
  showCreateCapabilitiesInfo = true;
  showSelectRequirementsWarning = true;
  showSelectEntityInfo = true;

  entityForm: CdFormGroup;

  readonly userEntityHelperText = $localize`Ceph Authentication entity used by mirroring.`;

  @Input() selectedFilesystem: FilesystemRow | null = null;
  @Output() entitySelected = new EventEmitter<string | null>();
  isSubmitting: boolean = false;

  private cephfsService = inject(CephfsService);
  private clusterService = inject(ClusterService);
  private taskWrapperService = inject(TaskWrapperService);
  private formBuilder = inject(CdFormBuilder);

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['selectedFilesystem'] && !changes['selectedFilesystem'].firstChange) {
      this.resetSelection();
    }
  }

  ngOnInit(): void {
    const noClientPrefix: ValidatorFn = (control: AbstractControl): ValidationErrors | null => {
      const value = (control.value ?? '').toString().trim();
      if (!value) return null;
      return value.startsWith('client.') ? { forbiddenClientPrefix: true } : null;
    };

    this.entityForm = this.formBuilder.group({
      user_entity: ['', [Validators.required, noClientPrefix]]
    });

    this.columns = [
      {
        name: $localize`Entity ID`,
        prop: 'entity',
        flexGrow: 2
      },
      {
        name: $localize`MDS capabilities`,
        prop: 'mdsCaps',
        flexGrow: 1.5
      },
      {
        name: $localize`MON capabilities`,
        prop: 'monCaps',
        flexGrow: 1.5
      },
      {
        name: $localize`OSD capabilities`,
        prop: 'osdCaps',
        flexGrow: 1.5
      }
    ];

    this.entities$ = this.subject$.pipe(
      switchMap(() =>
        this.clusterService.listUser().pipe(
          switchMap((users) => {
            const typedUsers = (users as CephAuthUser[]) || [];
            const filteredEntities = typedUsers.filter((entity) => {
              if (entity.entity?.startsWith('client.')) {
                const caps = entity.caps || {};
                const mdsCaps = caps.mds || '-';

                const fsName = this.selectedFilesystem?.name || '';
                const isValid = mdsCaps.includes(`fsname=${fsName}`);

                return isValid;
              }
              return false;
            });

            const rows: MirroringEntityRow[] = filteredEntities.map((entity) => {
              const caps = entity.caps || {};
              const mdsCaps = caps.mds || '-';
              const monCaps = caps.mon || '-';
              const osdCaps = caps.osd || '-';

              return {
                entity: entity.entity,
                mdsCaps,
                monCaps,
                osdCaps
              };
            });

            return of(rows);
          }),
          catchError(() => {
            this.context?.error();
            return of([]);
          })
        )
      )
    );

    this.loadEntities();
  }

  submitAction(): void {
    if (!this.entityForm.valid) {
      this.entityForm.markAllAsTouched();
      return;
    }

    const clientEntity = (this.entityForm.get('user_entity')?.value || '').toString().trim();
    const fullEntity = `client.${clientEntity}`;
    const fsName = this.selectedFilesystem?.name;

    const payload = {
      user_entity: fullEntity,
      capabilities: [
        { entity: 'mds', cap: 'allow *' },
        { entity: 'mgr', cap: 'allow *' },
        { entity: 'mon', cap: 'allow *' },
        { entity: 'osd', cap: 'allow *' }
      ]
    };

    this.isSubmitting = true;

    this.taskWrapperService
      .wrapTaskAroundCall({
        task: new FinishedTask(`ceph-user/create`, {
          userEntity: fullEntity,
          fsName: fsName
        }),
        call: this.clusterService.createUser(payload).pipe(
          map((res) => {
            return { ...(res as Record<string, unknown>), __taskCompleted: true };
          })
        )
      })
      .pipe(
        defaultIfEmpty(null),
        switchMap(() => {
          if (fsName) {
            return this.cephfsService.setAuth(fsName, clientEntity, ['/', 'rwps'], false);
          }
          return of(null);
        })
      )
      .subscribe({
        complete: () => {
          this.isSubmitting = false;
          this.entityForm.reset();
          this.handleEntityCreated(fullEntity);
        }
      });
  }

  private handleEntityCreated(entityId: string) {
    this.loadEntities(this.context);
    this.entitySelected.emit(entityId);
    this.isCreatingNewEntity = false;
  }

  loadEntities(context?: CdTableFetchDataContext) {
    this.context = context;
    this.subject$.next();
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
    const selectedRow = selection?.first();
    if (!selectedRow) {
      this.selection.selected = [];
      this.entitySelected.emit(null);
      return;
    }
    this.entitySelected.emit(selectedRow.entity);
  }

  resetSelection() {
    if (this.table) {
      this.table.model.selectAll(false);
    }
    this.selection = new CdTableSelection();
    this.entitySelected.emit(null);
  }

  onCreateEntitySelected() {
    this.isCreatingNewEntity = true;
    this.showCreateRequirementsWarning = true;
    this.showCreateCapabilitiesInfo = true;
  }

  onExistingEntitySelected() {
    this.isCreatingNewEntity = false;
    this.showSelectRequirementsWarning = true;
    this.showSelectEntityInfo = true;
    this.resetSelection();
  }

  onDismissCreateRequirementsWarning() {
    this.showCreateRequirementsWarning = false;
  }

  onDismissCreateCapabilitiesInfo() {
    this.showCreateCapabilitiesInfo = false;
  }

  onDismissSelectRequirementsWarning() {
    this.showSelectRequirementsWarning = false;
  }

  onDismissSelectEntityInfo() {
    this.showSelectEntityInfo = false;
  }
}
