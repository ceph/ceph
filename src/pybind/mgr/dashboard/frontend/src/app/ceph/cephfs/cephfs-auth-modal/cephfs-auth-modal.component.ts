import {
  AfterViewInit,
  ChangeDetectorRef,
  Component,
  Inject,
  OnInit,
  Optional
} from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { OperatorFunction, Observable, of } from 'rxjs';
import { debounceTime, distinctUntilChanged, switchMap, catchError } from 'rxjs/operators';
import { CephfsService } from '~/app/shared/api/cephfs.service';
import { DirectoryStoreService } from '~/app/shared/api/directory-store.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { PERMISSION_NAMES } from '~/app/shared/models/cephfs.model';

const DEBOUNCE_TIMER = 300;

@Component({
  selector: 'cd-cephfs-auth-modal',
  templateUrl: './cephfs-auth-modal.component.html',
  styleUrls: ['./cephfs-auth-modal.component.scss']
})
export class CephfsAuthModalComponent extends CdForm implements OnInit, AfterViewInit {
  subvolumeGroup: string;
  subvolume: string;
  isDefaultSubvolumeGroup = false;
  isSubvolume = false;
  form: CdFormGroup;
  action: string;
  resource: string;
  icons = Icons;
  readonly defaultdir: string = '/';

  clientPermissions = [
    {
      name: PERMISSION_NAMES.READ,
      description: $localize`Read permission is the minimum givable access`
    },
    {
      name: PERMISSION_NAMES.WRITE,
      description: $localize`Permission to set layouts or quotas, write access needed`
    },
    {
      name: PERMISSION_NAMES.QUOTA,
      description: $localize`Permission to set layouts or quotas, write access needed`
    },
    {
      name: PERMISSION_NAMES.SNAPSHOT,
      description: $localize`Permission to create or delete snapshots, write access needed`
    },
    {
      name: PERMISSION_NAMES.ROOTSQUASH,
      description: $localize`Safety measure to prevent scenarios such as accidental sudo rm -rf /path`
    }
  ];

  constructor(
    private actionLabels: ActionLabelsI18n,
    public directoryStore: DirectoryStoreService,
    private cephfsService: CephfsService,
    private taskWrapper: TaskWrapperService,
    private modalService: ModalCdsService,
    private changeDetectorRef: ChangeDetectorRef,

    @Optional() @Inject('fsName') public fsName: string,
    @Optional() @Inject('id') public id: number
  ) {
    super();
    this.action = this.actionLabels.UPDATE;
    this.resource = $localize`access`;
  }

  ngAfterViewInit(): void {
    this.changeDetectorRef.detectChanges();
  }

  ngOnInit() {
    this.directoryStore.loadDirectories(this.id, '/', 3);
    this.createForm();
    this.loadingReady();
  }

  createForm() {
    this.form = new CdFormGroup({
      fsName: new FormControl(
        { value: this.fsName, disabled: true },
        {
          validators: [Validators.required]
        }
      ),
      directory: new FormControl(
        { value: this.defaultdir, disabled: false },
        {
          updateOn: 'blur',
          validators: [Validators.required]
        }
      ),
      userId: new FormControl(undefined, {
        validators: [Validators.required]
      }),
      read: new FormControl(
        { value: true, disabled: true },
        {
          validators: [Validators.required]
        }
      ),
      write: new FormControl(undefined),
      snapshot: new FormControl({ value: false, disabled: true }),
      quota: new FormControl({ value: false, disabled: true }),
      rootSquash: new FormControl(undefined)
    });
  }

  search: OperatorFunction<string, readonly string[]> = (input: Observable<string>) =>
    input.pipe(
      debounceTime(DEBOUNCE_TIMER),
      distinctUntilChanged(),
      switchMap((term) =>
        this.directoryStore.search(term, this.id).pipe(
          catchError(() => {
            return of([]);
          })
        )
      )
    );

  onSubmit() {
    const clientId: number = this.form.getValue('userId');
    const caps: string[] = [this.form.getValue('directory'), this.transformPermissions()];
    const rootSquash: boolean = this.form.getValue(PERMISSION_NAMES.ROOTSQUASH);
    this.taskWrapper
      .wrapTaskAroundCall({
        task: new FinishedTask('cephfs/auth', {
          clientId: clientId
        }),
        call: this.cephfsService.setAuth(this.fsName, clientId, caps, rootSquash)
      })
      .subscribe({
        error: () => this.form.setErrors({ cdSubmitButton: true }),
        complete: () => {
          this.modalService.dismissAll();
        }
      });
  }

  transformPermissions(): string {
    const write = this.form.getValue(PERMISSION_NAMES.WRITE);
    const snapshot = this.form.getValue(PERMISSION_NAMES.SNAPSHOT);
    const quota = this.form.getValue(PERMISSION_NAMES.QUOTA);
    return `r${write ? 'w' : ''}${quota ? 'p' : ''}${snapshot ? 's' : ''}`;
  }

  toggleFormControl(_event?: boolean, permisson?: string) {
    const snapshot = this.form.get(PERMISSION_NAMES.SNAPSHOT);
    const quota = this.form.get(PERMISSION_NAMES.QUOTA);
    if (_event && permisson == PERMISSION_NAMES.WRITE) {
      snapshot.disabled ? snapshot.enable() : snapshot.disable();
      quota.disabled ? quota.enable() : quota.disable();
    } else if (!_event && permisson == PERMISSION_NAMES.WRITE) {
      snapshot.setValue(false);
      quota.setValue(false);
      snapshot.disable();
      quota.disable();
    }
  }
}
