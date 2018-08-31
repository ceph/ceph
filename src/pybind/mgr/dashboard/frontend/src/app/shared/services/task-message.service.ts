import { Injectable } from '@angular/core';

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
  operation: TaskMessageOperation;
  involves: (object) => string;
  errors: (metadata) => object;

  failure(metadata): string {
    return `Failed to ${this.operation.failure} ${this.involves(metadata)}`;
  }

  running(metadata): string {
    return `${this.operation.running} ${this.involves(metadata)}`;
  }

  success(metadata): string {
    return `${this.operation.success} ${this.involves(metadata)}`;
  }

  constructor(
    operation: TaskMessageOperation,
    involves: (metadata) => string,
    errors?: (metadata) => object
  ) {
    this.operation = operation;
    this.involves = involves;
    this.errors = errors || (() => ({}));
  }
}

@Injectable({
  providedIn: ServicesModule
})
export class TaskMessageService {
  defaultMessage = new TaskMessage(
    new TaskMessageOperation('Executing', 'execute', 'Executed'),
    (metadata) => {
      return (metadata && (Components[metadata.component] || metadata.component)) || 'unknown task';
    },
    () => {
      return {};
    }
  );

  commonOperations = {
    create: new TaskMessageOperation('Creating', 'create', 'Created'),
    update: new TaskMessageOperation('Updating', 'update', 'Updated'),
    delete: new TaskMessageOperation('Deleting', 'delete', 'Deleted')
  };

  rbd = {
    default: (metadata) => `RBD '${metadata.pool_name}/${metadata.image_name}'`,
    child: (metadata) => `RBD '${metadata.child_pool_name}/${metadata.child_image_name}'`,
    destination: (metadata) => `RBD '${metadata.dest_pool_name}/${metadata.dest_image_name}'`,
    snapshot: (metadata) =>
      `RBD snapshot '${metadata.pool_name}/${metadata.image_name}@${metadata.snapshot_name}'`
  };

  messages = {
    'rbd/create': new TaskMessage(this.commonOperations.create, this.rbd.default, (metadata) => ({
      '17': `Name is already used by ${this.rbd.default(metadata)}.`
    })),
    'rbd/edit': new TaskMessage(this.commonOperations.update, this.rbd.default, (metadata) => ({
      '17': `Name is already used by ${this.rbd.default(metadata)}.`
    })),
    'rbd/delete': new TaskMessage(this.commonOperations.delete, this.rbd.default, (metadata) => ({
      '39': `${this.rbd.default(metadata)} contains snapshots.`
    })),
    'rbd/clone': new TaskMessage(
      new TaskMessageOperation('Cloning', 'clone', 'Cloned'),
      this.rbd.child,
      (metadata) => ({
        '17': `Name is already used by ${this.rbd.child(metadata)}.`,
        '22': `Snapshot of ${this.rbd.child(metadata)} must be protected.`
      })
    ),
    'rbd/copy': new TaskMessage(
      new TaskMessageOperation('Copying', 'copy', 'Copied'),
      this.rbd.destination,
      (metadata) => ({
        '17': `Name is already used by ${this.rbd.destination(metadata)}.`
      })
    ),
    'rbd/flatten': new TaskMessage(
      new TaskMessageOperation('Flattening', 'flatten', 'Flattened'),
      this.rbd.default
    ),
    'rbd/snap/create': new TaskMessage(
      this.commonOperations.create,
      this.rbd.snapshot,
      (metadata) => ({
        '17': `Name is already used by ${this.rbd.snapshot(metadata)}.`
      })
    ),
    'rbd/snap/edit': new TaskMessage(
      this.commonOperations.update,
      this.rbd.snapshot,
      (metadata) => ({
        '16': `Cannot unprotect ${this.rbd.snapshot(metadata)} because it contains child images.`
      })
    ),
    'rbd/snap/delete': new TaskMessage(
      this.commonOperations.delete,
      this.rbd.snapshot,
      (metadata) => ({
        '16': `Cannot delete ${this.rbd.snapshot(metadata)} because it's protected.`
      })
    ),
    'rbd/snap/rollback': new TaskMessage(
      new TaskMessageOperation('Rolling back', 'rollback', 'Rolled back'),
      this.rbd.snapshot
    )
  };

  constructor() {}

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
