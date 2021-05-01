import { Injectable } from '@angular/core';

import { environment } from '~/environments/environment';

export class AppConstants {
  public static readonly organization = 'ceph';
  public static readonly projectName = 'Ceph Dashboard';
  public static readonly license = 'Free software (LGPL 2.1).';
  public static readonly copyright = 'Copyright(c) ' + environment.year + ' Ceph contributors.';
  public static readonly cephLogo = 'assets/Ceph_Logo.svg';
}

export enum URLVerbs {
  /* Create a new item */
  CREATE = 'create',

  /* Make changes to an existing item */
  EDIT = 'edit',

  /* Make changes to an existing item */
  UPDATE = 'update',

  /* Remove an item from a container WITHOUT deleting it */
  REMOVE = 'remove',

  /* Destroy an existing item */
  DELETE = 'delete',

  /* Add an existing item to a container */
  ADD = 'add',

  /* Non-standard verbs */
  COPY = 'copy',
  CLONE = 'clone',

  /* Prometheus wording */
  RECREATE = 'recreate',
  EXPIRE = 'expire'
}

export enum ActionLabels {
  /* Create a new item */
  CREATE = 'Create',

  /* Destroy an existing item */
  DELETE = 'Delete',

  /* Add an existing item to a container */
  ADD = 'Add',

  /* Remove an item from a container WITHOUT deleting it */
  REMOVE = 'Remove',

  /* Make changes to an existing item */
  EDIT = 'Edit',

  /* */
  CANCEL = 'Cancel',

  /* Non-standard actions */
  COPY = 'Copy',
  CLONE = 'Clone',
  UPDATE = 'Update',
  EVICT = 'Evict',

  /* Read-only */
  SHOW = 'Show',

  /* Prometheus wording */
  RECREATE = 'Recreate',
  EXPIRE = 'Expire'
}

@Injectable({
  providedIn: 'root'
})
export class ActionLabelsI18n {
  /* This service is required as the i18n polyfill does not provide static
  translation
  */
  CREATE: string;
  DELETE: string;
  ADD: string;
  REMOVE: string;
  EDIT: string;
  CANCEL: string;
  PREVIEW: string;
  MOVE: string;
  NEXT: string;
  BACK: string;
  CHANGE: string;
  COPY: string;
  CLONE: string;
  DEEP_SCRUB: string;
  DESTROY: string;
  EVICT: string;
  EXPIRE: string;
  FLATTEN: string;
  MARK_DOWN: string;
  MARK_IN: string;
  MARK_LOST: string;
  MARK_OUT: string;
  PROTECT: string;
  PURGE: string;
  RECREATE: string;
  RENAME: string;
  RESTORE: string;
  REWEIGHT: string;
  ROLLBACK: string;
  SCRUB: string;
  SET: string;
  SUBMIT: string;
  SHOW: string;
  TRASH: string;
  UNPROTECT: string;
  UNSET: string;
  UPDATE: string;
  FLAGS: string;
  ENTER_MAINTENANCE: string;
  EXIT_MAINTENANCE: string;

  constructor() {
    /* Create a new item */
    this.CREATE = $localize`Create`;

    /* Destroy an existing item */
    this.DELETE = $localize`Delete`;

    /* Add an existing item to a container */
    this.ADD = $localize`Add`;
    this.SET = $localize`Set`;
    this.SUBMIT = $localize`Submit`;

    /* Remove an item from a container WITHOUT deleting it */
    this.REMOVE = $localize`Remove`;
    this.UNSET = $localize`Unset`;

    /* Make changes to an existing item */
    this.EDIT = $localize`Edit`;
    this.UPDATE = $localize`Update`;
    this.CANCEL = $localize`Cancel`;
    this.PREVIEW = $localize`Preview`;
    this.MOVE = $localize`Move`;

    /* Wizard wording */
    this.NEXT = $localize`Next`;
    this.BACK = $localize`Back`;

    /* Non-standard actions */
    this.CLONE = $localize`Clone`;
    this.COPY = $localize`Copy`;
    this.DEEP_SCRUB = $localize`Deep Scrub`;
    this.DESTROY = $localize`Destroy`;
    this.EVICT = $localize`Evict`;
    this.FLATTEN = $localize`Flatten`;
    this.MARK_DOWN = $localize`Mark Down`;
    this.MARK_IN = $localize`Mark In`;
    this.MARK_LOST = $localize`Mark Lost`;
    this.MARK_OUT = $localize`Mark Out`;
    this.PROTECT = $localize`Protect`;
    this.PURGE = $localize`Purge`;
    this.RENAME = $localize`Rename`;
    this.RESTORE = $localize`Restore`;
    this.REWEIGHT = $localize`Reweight`;
    this.ROLLBACK = $localize`Rollback`;
    this.SCRUB = $localize`Scrub`;
    this.SHOW = $localize`Show`;
    this.TRASH = $localize`Move to Trash`;
    this.UNPROTECT = $localize`Unprotect`;
    this.CHANGE = $localize`Change`;
    this.FLAGS = $localize`Flags`;
    this.ENTER_MAINTENANCE = $localize`Enter Maintenance`;
    this.EXIT_MAINTENANCE = $localize`Exit Maintenance`;

    /* Prometheus wording */
    this.RECREATE = $localize`Recreate`;
    this.EXPIRE = $localize`Expire`;
  }
}

@Injectable({
  providedIn: 'root'
})
export class SucceededActionLabelsI18n {
  /* This service is required as the i18n polyfill does not provide static
  translation
  */
  CREATED: string;
  DELETED: string;
  ADDED: string;
  REMOVED: string;
  EDITED: string;
  CANCELED: string;
  PREVIEWED: string;
  MOVED: string;
  COPIED: string;
  CLONED: string;
  DEEP_SCRUBBED: string;
  DESTROYED: string;
  FLATTENED: string;
  MARKED_DOWN: string;
  MARKED_IN: string;
  MARKED_LOST: string;
  MARKED_OUT: string;
  PROTECTED: string;
  PURGED: string;
  RENAMED: string;
  RESTORED: string;
  REWEIGHTED: string;
  ROLLED_BACK: string;
  SCRUBBED: string;
  SHOWED: string;
  TRASHED: string;
  UNPROTECTED: string;
  CHANGE: string;
  RECREATED: string;
  EXPIRED: string;

  constructor() {
    /* Create a new item */
    this.CREATED = $localize`Created`;

    /* Destroy an existing item */
    this.DELETED = $localize`Deleted`;

    /* Add an existing item to a container */
    this.ADDED = $localize`Added`;

    /* Remove an item from a container WITHOUT deleting it */
    this.REMOVED = $localize`Removed`;

    /* Make changes to an existing item */
    this.EDITED = $localize`Edited`;
    this.CANCELED = $localize`Canceled`;
    this.PREVIEWED = $localize`Previewed`;
    this.MOVED = $localize`Moved`;

    /* Non-standard actions */
    this.CLONED = $localize`Cloned`;
    this.COPIED = $localize`Copied`;
    this.DEEP_SCRUBBED = $localize`Deep Scrubbed`;
    this.DESTROYED = $localize`Destroyed`;
    this.FLATTENED = $localize`Flattened`;
    this.MARKED_DOWN = $localize`Marked Down`;
    this.MARKED_IN = $localize`Marked In`;
    this.MARKED_LOST = $localize`Marked Lost`;
    this.MARKED_OUT = $localize`Marked Out`;
    this.PROTECTED = $localize`Protected`;
    this.PURGED = $localize`Purged`;
    this.RENAMED = $localize`Renamed`;
    this.RESTORED = $localize`Restored`;
    this.REWEIGHTED = $localize`Reweighted`;
    this.ROLLED_BACK = $localize`Rolled back`;
    this.SCRUBBED = $localize`Scrubbed`;
    this.SHOWED = $localize`Showed`;
    this.TRASHED = $localize`Moved to Trash`;
    this.UNPROTECTED = $localize`Unprotected`;
    this.CHANGE = $localize`Change`;

    /* Prometheus wording */
    this.RECREATED = $localize`Recreated`;
    this.EXPIRED = $localize`Expired`;
  }
}
