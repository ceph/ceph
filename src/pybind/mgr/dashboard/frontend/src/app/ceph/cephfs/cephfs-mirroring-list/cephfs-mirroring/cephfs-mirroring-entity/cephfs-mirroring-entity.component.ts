import { Component, OnInit, ViewChild, Input } from '@angular/core';
import { BehaviorSubject, Observable, of } from 'rxjs';
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

@Component({
  selector: 'cd-cephfs-mirroring-entity',
  templateUrl: './cephfs-mirroring-entity.component.html',
  standalone: false,
  styleUrls: ['./cephfs-mirroring-entity.component.scss']
})
export class CephfsMirroringEntityComponent extends CdForm implements OnInit {
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

  @Input() selectedFilesystem: string;

  constructor(private cephfsService: CephfsService, private formBuilder: CdFormBuilder) {
    super();
  }
  @Input() selectedFilesystem$: BehaviorSubject<any>;

  ngOnInit(): void {
    this.selectedFilesystem$?.subscribe((fs) => {
      console.log('Received filesystem:', fs);
      // use fs as needed
    });
    this.entityForm = this.formBuilder.group({
      entityName: ['', Validators.required]
    });

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
            const rows = users
              .filter((u) => u.entity?.startsWith('client.'))
              .map((user) => {
                const caps = user.caps || {};

                const mdsCaps = caps.mds || '-';
                const monCaps = caps.mon || '-';
                const osdCaps = caps.osd || '-';

                const isValid =
                  mdsCaps.includes('rw') || mdsCaps.includes('rwps') || osdCaps.includes('rw');

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
    if (this.entityForm.valid) {
      // const entityName = this.entityForm.get('entityName').value;
    }
  }

  loadEntities(context?: CdTableFetchDataContext) {
    this.context = context;
    this.subject$.next();
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }
}
