import { Injectable } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';

export class AppConstants {
  public static readonly organization = 'ceph';
  public static readonly projectName = 'Ceph Manager Dashboard';
  public static readonly license = 'Free software (LGPL 2.1).';
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
  CLONE = 'clone'
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

  /* Read-only */
  SHOW = 'Show'
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
  COPY: string;
  CLONE: string;
  SHOW: string;

  constructor(private i18n: I18n) {
    /* Create a new item */
    this.CREATE = this.i18n('Create');

    /* Destroy an existing item */
    this.DELETE = this.i18n('Delete');

    /* Add an existing item to a container */
    this.ADD = this.i18n('Add');

    /* Remove an item from a container WITHOUT deleting it */
    this.REMOVE = this.i18n('Remove');

    /* Make changes to an existing item */
    this.EDIT = this.i18n('Edit');
    this.CANCEL = this.i18n('Cancel');

    /* Non-standard actions */
    this.COPY = this.i18n('Copy');
    this.CLONE = this.i18n('Clone');

    this.SHOW = this.i18n('Show');
  }
}
