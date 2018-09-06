import * as _ from 'lodash';

import { FinishedTask } from '../models/finished-task';
import { TaskException } from '../models/task-exception';
import { TaskMessageOperation, TaskMessageService } from './task-message.service';

describe('TaskManagerMessageService', () => {
  let service: TaskMessageService;
  let finishedTask: FinishedTask;

  beforeEach(() => {
    service = new TaskMessageService();
    finishedTask = new FinishedTask();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should get default description', () => {
    expect(service.getErrorTitle(finishedTask)).toBe('Failed to execute unknown task');
  });

  it('should get default running message', () => {
    expect(service.getRunningTitle(finishedTask)).toBe('Executing unknown task');
  });

  it('should get default running message with a set component', () => {
    finishedTask.metadata = { component: 'rbd' };
    expect(service.getRunningTitle(finishedTask)).toBe('Executing RBD');
  });

  it('should getSuccessMessage', () => {
    expect(service.getSuccessTitle(finishedTask)).toBe('Executed unknown task');
  });

  describe('defined tasks messages', () => {
    const testMessages = (operation: TaskMessageOperation, involves: string) => {
      expect(service.getRunningTitle(finishedTask)).toBe(operation.running + ' ' + involves);
      expect(service.getErrorTitle(finishedTask)).toBe(
        'Failed to ' + operation.failure + ' ' + involves
      );
      expect(service.getSuccessTitle(finishedTask)).toBe(operation.success + ' ' + involves);
    };

    const testCreate = (involves: string) => {
      testMessages(new TaskMessageOperation('Creating', 'create', 'Created'), involves);
    };

    const testUpdate = (involves: string) => {
      testMessages(new TaskMessageOperation('Updating', 'update', 'Updated'), involves);
    };

    const testDelete = (involves: string) => {
      testMessages(new TaskMessageOperation('Deleting', 'delete', 'Deleted'), involves);
    };

    const testErrorCode = (code: number, msg: string) => {
      finishedTask.exception = _.assign(new TaskException(), {
        code: code
      });
      expect(service.getErrorMessage(finishedTask)).toBe(msg);
    };

    describe('rbd tasks', () => {
      let defaultMsg: string;
      let childMsg: string;
      let destinationMsg: string;
      let snapMsg: string;

      beforeEach(() => {
        const metadata = {
          pool_name: 'somePool',
          image_name: 'someImage',
          snapshot_name: 'someSnapShot',
          dest_pool_name: 'someDestinationPool',
          dest_image_name: 'someDestinationImage',
          child_pool_name: 'someChildPool',
          child_image_name: 'someChildImage'
        };
        defaultMsg = `RBD '${metadata.pool_name}/${metadata.image_name}'`;
        childMsg = `RBD '${metadata.child_pool_name}/${metadata.child_image_name}'`;
        destinationMsg = `RBD '${metadata.dest_pool_name}/${metadata.dest_image_name}'`;
        snapMsg = `RBD snapshot '${metadata.pool_name}/${metadata.image_name}@${
          metadata.snapshot_name
        }'`;
        finishedTask.metadata = metadata;
      });

      it('tests rbd/create messages', () => {
        finishedTask.name = 'rbd/create';
        testCreate(defaultMsg);
        testErrorCode(17, `Name is already used by ${defaultMsg}.`);
      });

      it('tests rbd/edit messages', () => {
        finishedTask.name = 'rbd/edit';
        testUpdate(defaultMsg);
        testErrorCode(17, `Name is already used by ${defaultMsg}.`);
      });

      it('tests rbd/delete messages', () => {
        finishedTask.name = 'rbd/delete';
        testDelete(defaultMsg);
        testErrorCode(39, `${defaultMsg} contains snapshots.`);
      });

      it('tests rbd/clone messages', () => {
        finishedTask.name = 'rbd/clone';
        testMessages(new TaskMessageOperation('Cloning', 'clone', 'Cloned'), childMsg);
        testErrorCode(17, `Name is already used by ${childMsg}.`);
        testErrorCode(22, `Snapshot of ${childMsg} must be protected.`);
      });

      it('tests rbd/copy messages', () => {
        finishedTask.name = 'rbd/copy';
        testMessages(new TaskMessageOperation('Copying', 'copy', 'Copied'), destinationMsg);
        testErrorCode(17, `Name is already used by ${destinationMsg}.`);
      });

      it('tests rbd/flatten messages', () => {
        finishedTask.name = 'rbd/flatten';
        testMessages(new TaskMessageOperation('Flattening', 'flatten', 'Flattened'), defaultMsg);
      });

      it('tests rbd/snap/create messages', () => {
        finishedTask.name = 'rbd/snap/create';
        testCreate(snapMsg);
        testErrorCode(17, `Name is already used by ${snapMsg}.`);
      });

      it('tests rbd/snap/edit messages', () => {
        finishedTask.name = 'rbd/snap/edit';
        testUpdate(snapMsg);
        testErrorCode(16, `Cannot unprotect ${snapMsg} because it contains child images.`);
      });

      it('tests rbd/snap/delete messages', () => {
        finishedTask.name = 'rbd/snap/delete';
        testDelete(snapMsg);
        testErrorCode(16, `Cannot delete ${snapMsg} because it's protected.`);
      });

      it('tests rbd/snap/rollback messages', () => {
        finishedTask.name = 'rbd/snap/rollback';
        testMessages(new TaskMessageOperation('Rolling back', 'rollback', 'Rolled back'), snapMsg);
      });
    });
  });
});
