import { Injectable } from '@angular/core';
import { FinishedTask } from '../models/finished-task';
import { Task } from '../models/task';

class TaskManagerMessage {
  descr: (metadata) => string;
  success: (metadata) => string;
  error: (metadata) => object;

  constructor(descr: (metadata) => string,
              success: (metadata) => string,
              error: (metadata) => object) {
    this.descr = descr;
    this.success = success;
    this.error = error;
  }
}

@Injectable()
export class TaskManagerMessageService {

  messages = {
    'rbd/create': new TaskManagerMessage(
      (metadata) => `Create RBD '${metadata.pool_name}/${metadata.image_name}'`,
      (metadata) => `RBD '${metadata.pool_name}/${metadata.image_name}'
                     has been created successfully`,
      (metadata) => {
        return {
          '17': `Name '${metadata.pool_name}/${metadata.image_name}' is already
                 in use.`
        };
      }
    ),
    'rbd/edit': new TaskManagerMessage(
      (metadata) => `Update RBD '${metadata.pool_name}/${metadata.image_name}'`,
      (metadata) => `RBD '${metadata.pool_name}/${metadata.image_name}'
                     has been updated successfully`,
      (metadata) => {
        return {
          '17': `Name '${metadata.pool_name}/${metadata.name}' is already
                 in use.`
        };
      }
    ),
    'rbd/delete': new TaskManagerMessage(
      (metadata) => `Delete RBD '${metadata.pool_name}/${metadata.image_name}'`,
      (metadata) => `RBD '${metadata.pool_name}/${metadata.image_name}'
                     has been deleted successfully`,
      (metadata) => {
        return {
          '39': `RBD image contains snapshots.`
        };
      }
    ),
    'rbd/clone': new TaskManagerMessage(
      (metadata) => `Clone RBD '${metadata.child_pool_name}/${metadata.child_image_name}'`,
      (metadata) => `RBD '${metadata.child_pool_name}/${metadata.child_image_name}'
                     has been cloned successfully`,
      (metadata) => {
        return {
          '17': `Name '${metadata.child_pool_name}/${metadata.child_image_name}' is already
                 in use.`,
          '22': `Snapshot must be protected.`
        };
      }
    ),
    'rbd/copy': new TaskManagerMessage(
      (metadata) => `Copy RBD '${metadata.dest_pool_name}/${metadata.dest_image_name}'`,
      (metadata) => `RBD '${metadata.dest_pool_name}/${metadata.dest_image_name}'
                     has been copied successfully`,
      (metadata) => {
        return {
          '17': `Name '${metadata.dest_pool_name}/${metadata.dest_image_name}' is already
                 in use.`
        };
      }
    ),
    'rbd/flatten': new TaskManagerMessage(
      (metadata) => `Flatten RBD '${metadata.pool_name}/${metadata.image_name}'`,
      (metadata) => `RBD '${metadata.pool_name}/${metadata.image_name}'
                     has been flattened successfully`,
      () => {
        return {
        };
      }
    ),
    'rbd/snap/create': new TaskManagerMessage(
      (metadata) => `Create snapshot ` +
                    `'${metadata.pool_name}/${metadata.image_name}@${metadata.snapshot_name}'`,
      (metadata) => `Snapshot ` +
                    `'${metadata.pool_name}/${metadata.image_name}@${metadata.snapshot_name}' ` +
                    `has been created successfully`,
      (metadata) => {
        return {
          '17': `Name '${metadata.snapshot_name}' is already in use.`
        };
      }
    ),
    'rbd/snap/edit': new TaskManagerMessage(
      (metadata) => `Update snapshot ` +
                    `'${metadata.pool_name}/${metadata.image_name}@${metadata.snapshot_name}'`,
      (metadata) => `Snapshot ` +
                    `'${metadata.pool_name}/${metadata.image_name}@${metadata.snapshot_name}' ` +
                    `has been updated successfully`,
      () => {
        return {
          '16': `Cannot unprotect snapshot because it contains child images.`
        };
      }
    ),
    'rbd/snap/delete': new TaskManagerMessage(
      (metadata) => `Delete snapshot ` +
                    `'${metadata.pool_name}/${metadata.image_name}@${metadata.snapshot_name}'`,
      (metadata) => `Snapshot ` +
                    `'${metadata.pool_name}/${metadata.image_name}@${metadata.snapshot_name}' ` +
                    `has been deleted successfully`,
      () => {
        return {
          '16': `Snapshot is protected.`
        };
      }
    ),
    'rbd/snap/rollback': new TaskManagerMessage(
      (metadata) => `Rollback snapshot ` +
                    `'${metadata.pool_name}/${metadata.image_name}@${metadata.snapshot_name}'`,
      (metadata) => `Snapshot ` +
                    `'${metadata.pool_name}/${metadata.image_name}@${metadata.snapshot_name}' ` +
                    `has been rolled back successfully`,
      () => {
        return {
        };
      }
    ),
  };

  defaultMessage = new TaskManagerMessage(
    (metadata) => 'Unknown Task',
    (metadata) => 'Task executed successfully',
    () => {
      return {
      };
    }
  );

  constructor() { }

  getSuccessMessage(finishedTask: FinishedTask) {
    const taskManagerMessage = this.messages[finishedTask.name] || this.defaultMessage;
    return taskManagerMessage.success(finishedTask.metadata);
  }

  getErrorMessage(finishedTask: FinishedTask) {
    const taskManagerMessage = this.messages[finishedTask.name] || this.defaultMessage;
    return taskManagerMessage.error(finishedTask.metadata)[finishedTask.exception.errno] ||
      finishedTask.exception.detail;
  }

  getDescription(task: Task) {
    const taskManagerMessage = this.messages[task.name] || this.defaultMessage;
    return taskManagerMessage.descr(task.metadata);
  }
}
