import { Component, OnInit, ViewChild, Input, OnDestroy } from '@angular/core';
import { BehaviorSubject, Observable, of, Subscription } from 'rxjs';
import { catchError, map, switchMap } from 'rxjs/operators';

import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { CephfsService } from '~/app/shared/api/cephfs.service';

import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { Validators } from '@angular/forms';
import { CdForm } from '~/app/shared/forms/cd-form';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { FinishedTask } from '~/app/shared/models/finished-task';
//import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
//import { FinishedTask } from '~/app/shared/models/finished-task';

@Component({
  selector: 'cd-cephfs-mirroring-entity',
  templateUrl: './cephfs-mirroring-entity.component.html',
  standalone: false
})
export class CephfsMirroringEntityComponent extends CdForm implements OnInit, OnDestroy {
  @ViewChild('table', { static: true }) table: TableComponent;
  columns: CdTableColumn[];
  selection = new CdTableSelection();

  subject$ = new BehaviorSubject<void>(undefined);
  entities$: Observable<any[]>;
  context: CdTableFetchDataContext;
  capabilities = [
    { name: 'MDS', permission: 'rwps' },
    { name: 'MON', permission: 'rwps' },
    { name: 'OSD', permission: 'rwps' }
  ];

  isCreatingNewEntity = true;

  entityForm: CdFormGroup;

  @Input() selectedFilesystem$: BehaviorSubject<any>;
  selectedFilesystem: any = null;
  private _fsSub: Subscription;
  isSubmitting: boolean = false

  constructor(private cephfsService: CephfsService,
       private taskWrapperService: TaskWrapperService,
       private formBuilder: CdFormBuilder) {
    super();
  }

  ngOnInit(): void {
    // Subscribe to selectedFilesystem$ to get the selected filesystem
    this._fsSub = this.selectedFilesystem$?.subscribe((fs) => {
      this.selectedFilesystem = fs;
      console.log('Selected filesystem in entity component:', fs);  
      // optionally prefill form field
    });

    // Initialize the form
    this.entityForm = this.formBuilder.group({
      user_entity: ['', Validators.required]
    });

    // Define table columns
    this.columns = [
      {
        name: $localize`Entity ID`,
        prop: 'entity',
        flexGrow: 2
      },
      {
        name: $localize`MDS Capabilities`,
        prop: 'mdsCaps',
        flexGrow: 1.5
      },
      {
        name: $localize`MON Capabilities`,
        prop: 'monCaps',
        flexGrow: 1.5
      },
      {
        name: $localize`OSD Capabilities`,
        prop: 'osdCaps',
        flexGrow: 1.5
      },
      {
        name: $localize`Status`,
        prop: 'status',
        flexGrow: 0.8
      }
    ];

 this.entities$ = this.subject$.pipe(
      switchMap(() =>
        this.cephfsService.listUser().pipe(
          switchMap((users) => {
            const filteredUsers = users.filter((user) => {
              // Check if the user has a valid entity (starts with 'client.')
              if (user.entity?.startsWith('client.')) {
                const caps = user.caps || {};
                const mdsCaps = caps.mds || '-';

                const fsName = this.selectedFilesystem?.name || '';

                // Check if fsName matches the fsname in caps.mds
                const isValid =
                  mdsCaps.includes(`fsname=${fsName}`);

                return isValid;
              }
              return false;
            });

            // Map filtered users to table row format
            const rows = filteredUsers.map((user) => {
              const caps = user.caps || {};
              const mdsCaps = caps.mds || '-';
              const monCaps = caps.mon || '-';
              const osdCaps = caps.osd || '-';
              const isValid = mdsCaps.includes('rw') || mdsCaps.includes('rwps') || osdCaps.includes('rw');

              return {
                entity: user.entity,
                mdsCaps,
                monCaps,
                osdCaps,
                status: isValid ? 'Valid' : 'Invalid'
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

  const userEntity = this.entityForm.get('user_entity')?.value;

  const payload = {
    user_entity: userEntity,
    capabilities: [
      { entity: 'mds', cap: 'allow *' },
      { entity: 'mgr', cap: 'allow *' },
      { entity: 'mon', cap: 'allow *' },
      { entity: 'osd', cap: 'allow *' }
    ]
  };

  this.isSubmitting = true;

  // Wrap the HTTP call in the taskWrapper, but make it complete immediately
  this.taskWrapperService
    .wrapTaskAroundCall({
      task: new FinishedTask(`mirroring/cephUser/create`, { userEntity }),
      call: this.cephfsService.createUser(payload)
        // Mark the task as finished immediately after HTTP succeeds
        .pipe(
          map(res => {
            return { ...res, __taskCompleted: true };
          })
        )
    })
    .subscribe({
      complete: () => {
        console.log('Ceph user created successfully');
        this.isSubmitting = false;
       this.entityForm.reset();
         this.loadEntities(this.context);
       // this.isCreatingNewEntity = false;
      },
      error: (err) => {
        console.error('Error creating Ceph user', err);
        this.isSubmitting = false;
        this.entityForm.setErrors({ submitFailed: true });
      }
    });
}




  loadEntities(context?: CdTableFetchDataContext) {
    this.context = context;
    this.subject$.next();
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  ngOnDestroy(): void {
    this._fsSub?.unsubscribe();
  }
}
