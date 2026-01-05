import { Component, OnInit, ViewChild, Input, OnDestroy } from '@angular/core';
import { BehaviorSubject, Observable, of, Subscription } from 'rxjs';
import { catchError, switchMap } from 'rxjs/operators';

import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { CephfsService } from '~/app/shared/api/cephfs.service';

import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { Validators } from '@angular/forms';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CephUserService } from '~/app/shared/api/ceph-user.service';

@Component({
  selector: 'cd-cephfs-mirroring-entity',
  templateUrl: './cephfs-mirroring-entity.component.html'
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

  constructor(private cephfsService: CephfsService, private formBuilder: CdFormBuilder) {
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
              if (user.entity?.startsWith('client.')) {
                const caps = user.caps || {};
                const mdsCaps = caps.mds || '-';
               // const monCaps = caps.mon || '-';
                const osdCaps = caps.osd || '-';

                // Filter users based on the selected filesystem name
                const fsName = this.selectedFilesystem;
                const isValid =
                  (mdsCaps.includes('rw') || mdsCaps.includes('rwps') || osdCaps.includes('rw')) &&
                  user.entity.includes(fsName);

                return isValid;
              }
              return false;
            });

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

            console.log('Loaded entities:', rows);
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

  // Handle submit action (e.g., when the user creates a new entity)

  // {"capabilities":[{"entity":"mds","cap":"allow *"},{"entity":"mgr","cap":"allow *"},
  // {"entity":"mon","cap":"allow *"},{"entity":"osd","cap":"allow *"}]
  // ,"user_entity":"client.mirror45"}
submitAction(): void {
  console.log('Submit action triggered');
    // Set the loading state to true to show the submit button is busy
    this.isSubmitting = true;

    const userEntity = this.entityForm.get('entityName').value;

    // Define the default capabilities
    const defaultCapabilities = [
      { entity: 'mds', cap: 'allow rw' },
      { entity: 'mon', cap: 'allow rw' },
      { entity: 'osd', cap: 'allow rw' }
    ];
  console.log('Creating entity with values:', userEntity, defaultCapabilities);
    // Call the service to create the user with default capabilities
    this.cephfsService.createUser(userEntity, defaultCapabilities).subscribe(
      (response) => {
        console.log('Entity created successfully:', response);
        this.isCreatingNewEntity = false;
        this.loadEntities(this.context); // Reload entities after creation
        this.isSubmitting = false; // Reset the loading state
      },
      (error) => {
        console.error('Error creating entity:', error);
        this.isSubmitting = false; // Reset the loading state
      }
    );
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
