import { Component, OnInit } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { OperatorFunction, Observable, of } from 'rxjs';
import { debounceTime, distinctUntilChanged, switchMap, catchError } from 'rxjs/operators';
import { CephfsService } from '~/app/shared/api/cephfs.service';
import { DirectoryStoreService } from '~/app/shared/api/directory-store.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';

const DEBOUNCE_TIMER = 300;

@Component({
  selector: 'cd-cephfs-auth-modal',
  templateUrl: './cephfs-auth-modal.component.html',
  styleUrls: ['./cephfs-auth-modal.component.scss']
})
export class CephfsAuthModalComponent extends CdForm implements OnInit {
  fsName: string;
  id: number;
  subvolumeGroup: string;
  subvolume: string;
  isDefaultSubvolumeGroup = false;
  isSubvolume = false;
  form: CdFormGroup;
  action: string;
  resource: string;
  icons = Icons;

  constructor(
    public activeModal: NgbActiveModal,
    private actionLabels: ActionLabelsI18n,
    public directoryStore: DirectoryStoreService,
    private cephfsService: CephfsService,
    private taskWrapper: TaskWrapperService
  ) {
    super();
    this.action = this.actionLabels.UPDATE;
    this.resource = $localize`access`;
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
      directory: new FormControl(undefined, {
        updateOn: 'blur',
        validators: [Validators.required]
      }),
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

  closeModal() {
    this.activeModal.close();
  }

  onSubmit() {
    const clientId: number = this.form.getValue('userId');
    const caps: string[] = [this.form.getValue('directory'), this.transformPermissions()];
    const rootSquash: boolean = this.form.getValue('rootSquash');
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
          this.activeModal.close();
        }
      });
  }

  transformPermissions(): string {
    const write = this.form.getValue('write');
    const snapshot = this.form.getValue('snapshot');
    const quota = this.form.getValue('quota');
    return `r${write ? 'w' : ''}${quota ? 'p' : ''}${snapshot ? 's' : ''}`;
  }

  toggleFormControl() {
    const snapshot = this.form.get('snapshot');
    const quota = this.form.get('quota');
    snapshot.disabled ? snapshot.enable() : snapshot.disable();
    quota.disabled ? quota.enable() : quota.disable();
  }
}
