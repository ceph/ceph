import { Injectable } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';

import { Components } from '../enum/components.enum';
import { FinishedTask } from '../models/finished-task';
import { Task } from '../models/task';
import { ServicesModule } from './services.module';

export class TaskMessageOperation {
  running: string;
  failure: string;
  success: string;

  constructor(running: string, failure: string, success: string) {
    this.running = running;
    this.failure = failure;
    this.success = success;
  }
}

class TaskMessage {
  i18n: I18n;

  operation: TaskMessageOperation;
  involves: (object) => string;
  errors: (metadata) => object;

  failure(metadata): string {
    return this.i18n('Failed to {{failure}} {{metadata}}', {
      failure: this.operation.failure,
      metadata: this.involves(metadata)
    });
  }

  running(metadata): string {
    return `${this.operation.running} ${this.involves(metadata)}`;
  }

  success(metadata): string {
    return `${this.operation.success} ${this.involves(metadata)}`;
  }

  constructor(
    i18n: I18n,
    operation: TaskMessageOperation,
    involves: (metadata) => string,
    errors?: (metadata) => object
  ) {
    this.i18n = i18n;
    this.operation = operation;
    this.involves = involves;
    this.errors = errors || (() => ({}));
  }
}

@Injectable({
  providedIn: ServicesModule
})
export class TaskMessageService {
  constructor(private i18n: I18n) {}

  defaultMessage = this.newTaskMessage(
    new TaskMessageOperation(this.i18n('Executing'), this.i18n('execute'), this.i18n('Executed')),
    (metadata) => {
      return (
        (metadata && (Components[metadata.component] || metadata.component)) ||
        this.i18n('unknown task')
      );
    },
    () => {
      return {};
    }
  );

  commonOperations = {
    create: new TaskMessageOperation(
      this.i18n('Creating'),
      this.i18n('create'),
      this.i18n('Created')
    ),
    update: new TaskMessageOperation(
      this.i18n('Updating'),
      this.i18n('update'),
      this.i18n('Updated')
    ),
    delete: new TaskMessageOperation(
      this.i18n('Deleting'),
      this.i18n('delete'),
      this.i18n('Deleted')
    )
  };

  rbd = {
    default: (metadata) =>
      this.i18n(`RBD '{{id}}'`, {
        id: `${metadata.pool_name}/${metadata.image_name}`
      }),
    child: (metadata) =>
      this.i18n(`RBD '{{id}}'`, {
        id: `${metadata.child_pool_name}/${metadata.child_image_name}`
      }),
    destination: (metadata) =>
      this.i18n(`RBD '{{id}}'`, {
        id: `${metadata.dest_pool_name}/${metadata.dest_image_name}`
      }),
    snapshot: (metadata) =>
      this.i18n(`RBD snapshot '{{id}}'`, {
        id: `${metadata.pool_name}/${metadata.image_name}@${metadata.snapshot_name}`
      })
  };

  rbd_mirroring = {
    pool: (metadata) =>
      this.i18n(`mirror mode for pool '{{id}}'`, {
        id: `${metadata.pool_name}`
      }),
    pool_peer: (metadata) =>
      this.i18n(`mirror peer for pool '{{id}}'`, {
        id: `${metadata.pool_name}`
      })
  };

  messages = {
    // Pool tasks
    'pool/create': this.newTaskMessage(
      this.commonOperations.create,
      (metadata) => this.pool(metadata),
      (metadata) => ({
        '17': this.i18n('Name is already used by {{pool_name}}.', {
          pool_name: this.pool(metadata)
        })
      })
    ),
    'pool/edit': this.newTaskMessage(
      this.commonOperations.update,
      (metadata) => this.pool(metadata),
      (metadata) => ({
        '17': this.i18n('Name is already used by {{pool_name}}.', {
          pool_name: this.pool(metadata)
        })
      })
    ),
    'pool/delete': this.newTaskMessage(this.commonOperations.delete, (metadata) =>
      this.pool(metadata)
    ),
    // Erasure code profile tasks
    'ecp/create': this.newTaskMessage(
      this.commonOperations.create,
      (metadata) => this.ecp(metadata),
      (metadata) => ({
        '17': this.i18n('Name is already used by {{name}}.', {
          name: this.ecp(metadata)
        })
      })
    ),
    'ecp/delete': this.newTaskMessage(this.commonOperations.delete, (metadata) =>
      this.ecp(metadata)
    ),
    // RBD tasks
    'rbd/create': this.newTaskMessage(
      this.commonOperations.create,
      this.rbd.default,
      (metadata) => ({
        '17': this.i18n('Name is already used by {{rbd_name}}.', {
          rbd_name: this.rbd.default(metadata)
        })
      })
    ),
    'rbd/edit': this.newTaskMessage(this.commonOperations.update, this.rbd.default, (metadata) => ({
      '17': this.i18n('Name is already used by {{rbd_name}}.', {
        rbd_name: this.rbd.default(metadata)
      })
    })),
    'rbd/delete': this.newTaskMessage(
      this.commonOperations.delete,
      this.rbd.default,
      (metadata) => ({
        '39': this.i18n('{{rbd_name}} contains snapshots.', {
          rbd_name: this.rbd.default(metadata)
        })
      })
    ),
    'rbd/clone': this.newTaskMessage(
      new TaskMessageOperation(this.i18n('Cloning'), this.i18n('clone'), this.i18n('Cloned')),
      this.rbd.child,
      (metadata) => ({
        '17': this.i18n('Name is already used by {{rbd_name}}.', {
          rbd_name: this.rbd.child(metadata)
        }),
        '22': this.i18n('Snapshot of {{rbd_name}} must be protected.', {
          rbd_name: this.rbd.child(metadata)
        })
      })
    ),
    'rbd/copy': this.newTaskMessage(
      new TaskMessageOperation(this.i18n('Copying'), this.i18n('copy'), this.i18n('Copied')),
      this.rbd.destination,
      (metadata) => ({
        '17': this.i18n('Name is already used by {{rbd_name}}.', {
          rbd_name: this.rbd.destination(metadata)
        })
      })
    ),
    'rbd/flatten': this.newTaskMessage(
      new TaskMessageOperation(
        this.i18n('Flattening'),
        this.i18n('flatten'),
        this.i18n('Flattened')
      ),
      this.rbd.default
    ),
    // RBD snapshot tasks
    'rbd/snap/create': this.newTaskMessage(
      this.commonOperations.create,
      this.rbd.snapshot,
      (metadata) => ({
        '17': this.i18n('Name is already used by {{snap_name}}.', {
          snap_name: this.rbd.snapshot(metadata)
        })
      })
    ),
    'rbd/snap/edit': this.newTaskMessage(
      this.commonOperations.update,
      this.rbd.snapshot,
      (metadata) => ({
        '16': this.i18n('Cannot unprotect {{snap_name}} because it contains child images.', {
          snap_name: this.rbd.snapshot(metadata)
        })
      })
    ),
    'rbd/snap/delete': this.newTaskMessage(
      this.commonOperations.delete,
      this.rbd.snapshot,
      (metadata) => ({
        '16': this.i18n(`Cannot delete {{snap_name}} because it's protected.`, {
          snap_name: this.rbd.snapshot(metadata)
        })
      })
    ),
    'rbd/snap/rollback': this.newTaskMessage(
      new TaskMessageOperation(
        this.i18n('Rolling back'),
        this.i18n('rollback'),
        this.i18n('Rolled back')
      ),
      this.rbd.snapshot
    ),
    // RBD trash tasks
    'rbd/trash/move': this.newTaskMessage(
      new TaskMessageOperation(this.i18n('Moving'), this.i18n('move'), this.i18n('Moved')),
      (metadata) =>
        this.i18n(`image '{{id}}' to trash`, {
          id: `${metadata.pool_name}/${metadata.image_name}`
        }),
      () => ({
        2: this.i18n('Could not find image.')
      })
    ),
    'rbd/trash/restore': this.newTaskMessage(
      new TaskMessageOperation(this.i18n('Restoring'), this.i18n('restore'), this.i18n('Restored')),
      (metadata) =>
        this.i18n(`image '{{id}}' into '{{new_id}}'`, {
          id: `${metadata.pool_name}@${metadata.image_id}`,
          new_id: `${metadata.pool_name}/${metadata.new_image_name}`
        }),
      (metadata) => ({
        17: this.i18n(`Image name '{{id}}' is already in use.`, {
          id: `${metadata.pool_name}/${metadata.new_image_name}`
        })
      })
    ),
    'rbd/trash/remove': this.newTaskMessage(
      new TaskMessageOperation(this.i18n('Deleting'), this.i18n('delete'), this.i18n('Deleted')),
      (metadata) =>
        this.i18n(`image '{{id}}'`, {
          id: `${metadata.pool_name}/${metadata.image_name}@${metadata.image_id}`
        })
    ),
    'rbd/trash/purge': this.newTaskMessage(
      new TaskMessageOperation(this.i18n('Purging'), this.i18n('purge'), this.i18n('Purged')),
      (metadata) => {
        let message = this.i18n('all pools');
        if (metadata.pool_name) {
          message = `'${metadata.pool_name}'`;
        }
        return this.i18n('images from {{message}}', {
          message: message
        });
      }
    ),
    // RBD mirroring tasks
    'rbd/mirroring/pool/edit': this.newTaskMessage(
      this.commonOperations.update,
      this.rbd_mirroring.pool,
      (metadata) => ({
        16: this.i18n('Cannot disable mirroring because it contains a peer.')
      })
    ),
    'rbd/mirroring/peer/add': this.newTaskMessage(
      this.commonOperations.create,
      this.rbd_mirroring.pool_peer,
      (metadata) => ({})
    ),
    'rbd/mirroring/peer/edit': this.newTaskMessage(
      this.commonOperations.update,
      this.rbd_mirroring.pool_peer,
      (metadata) => ({})
    ),
    'rbd/mirroring/peer/delete': this.newTaskMessage(
      this.commonOperations.delete,
      this.rbd_mirroring.pool_peer,
      (metadata) => ({})
    ),
    // iSCSI target tasks
    'iscsi/target/create': this.newTaskMessage(this.commonOperations.create, (metadata) =>
      this.iscsiTarget(metadata)
    ),
    'iscsi/target/edit': this.newTaskMessage(this.commonOperations.update, (metadata) =>
      this.iscsiTarget(metadata)
    ),
    'iscsi/target/delete': this.newTaskMessage(this.commonOperations.delete, (metadata) =>
      this.iscsiTarget(metadata)
    ),
    'nfs/create': this.newTaskMessage(this.commonOperations.create, (metadata) =>
      this.nfs(metadata)
    ),
    'nfs/edit': this.newTaskMessage(this.commonOperations.update, (metadata) => this.nfs(metadata)),
    'nfs/delete': this.newTaskMessage(this.commonOperations.delete, (metadata) =>
      this.nfs(metadata)
    )
  };

  newTaskMessage(
    operation: TaskMessageOperation,
    involves: (metadata) => string,
    errors?: (metadata) => object
  ) {
    return new TaskMessage(this.i18n, operation, involves, errors);
  }

  pool(metadata) {
    return this.i18n(`pool '{{pool_name}}'`, {
      pool_name: metadata.pool_name
    });
  }

  ecp(metadata) {
    return this.i18n(`erasure code profile '{{name}}'`, { name: metadata.name });
  }

  iscsiTarget(metadata) {
    return this.i18n(`target '{{target_iqn}}'`, { target_iqn: metadata.target_iqn });
  }

  nfs(metadata) {
    return this.i18n(`NFS {{nfs_id}}`, {
      nfs_id: `'${metadata.cluster_id}:${metadata.export_id ? metadata.export_id : metadata.path}'`
    });
  }

  _getTaskTitle(task: Task) {
    return this.messages[task.name] || this.defaultMessage;
  }

  getSuccessTitle(task: FinishedTask) {
    return this._getTaskTitle(task).success(task.metadata);
  }

  getErrorMessage(task: FinishedTask) {
    return (
      this._getTaskTitle(task).errors(task.metadata)[task.exception.code] || task.exception.detail
    );
  }

  getErrorTitle(task: Task) {
    return this._getTaskTitle(task).failure(task.metadata);
  }

  getRunningTitle(task: Task) {
    return this._getTaskTitle(task).running(task.metadata);
  }

  getRunningText(task: Task) {
    return this._getTaskTitle(task).operation.running;
  }
}
