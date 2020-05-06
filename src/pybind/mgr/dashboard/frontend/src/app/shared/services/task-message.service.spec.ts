import { TestBed } from '@angular/core/testing';

import * as _ from 'lodash';

import { HttpClientTestingModule } from '@angular/common/http/testing';
import { configureTestBed, i18nProviders } from '../../../testing/unit-test-helper';
import { RbdService } from '../api/rbd.service';
import { FinishedTask } from '../models/finished-task';
import { TaskException } from '../models/task-exception';
import { TaskMessageOperation, TaskMessageService } from './task-message.service';

describe('TaskManagerMessageService', () => {
  let service: TaskMessageService;
  let finishedTask: FinishedTask;

  configureTestBed({
    providers: [TaskMessageService, i18nProviders, RbdService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.inject(TaskMessageService);
    finishedTask = new FinishedTask();
    finishedTask.duration = 30;
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
    let defaultMsg: string;
    const testMessages = (operation: TaskMessageOperation, involves: string) => {
      expect(service.getRunningTitle(finishedTask)).toBe(operation.running + ' ' + involves);
      expect(service.getErrorTitle(finishedTask)).toBe(
        'Failed to ' + operation.failure + ' ' + involves
      );
      expect(service.getSuccessTitle(finishedTask)).toBe(`${operation.success} ${involves}`);
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

    const testImport = (involves: string) => {
      testMessages(new TaskMessageOperation('Importing', 'import', 'Imported'), involves);
    };

    const testErrorCode = (code: number, msg: string) => {
      finishedTask.exception = _.assign(new TaskException(), {
        code: code
      });
      expect(service.getErrorMessage(finishedTask)).toBe(msg);
    };

    describe('pool tasks', () => {
      beforeEach(() => {
        const metadata = {
          pool_name: 'somePool'
        };
        defaultMsg = `pool '${metadata.pool_name}'`;
        finishedTask.metadata = metadata;
      });

      it('tests pool/create messages', () => {
        finishedTask.name = 'pool/create';
        testCreate(defaultMsg);
        testErrorCode(17, `Name is already used by ${defaultMsg}.`);
      });

      it('tests pool/edit messages', () => {
        finishedTask.name = 'pool/edit';
        testUpdate(defaultMsg);
        testErrorCode(17, `Name is already used by ${defaultMsg}.`);
      });

      it('tests pool/delete messages', () => {
        finishedTask.name = 'pool/delete';
        testDelete(defaultMsg);
      });
    });

    describe('erasure code profile tasks', () => {
      beforeEach(() => {
        const metadata = {
          name: 'someEcpName'
        };
        defaultMsg = `erasure code profile '${metadata.name}'`;
        finishedTask.metadata = metadata;
      });

      it('tests ecp/create messages', () => {
        finishedTask.name = 'ecp/create';
        testCreate(defaultMsg);
        testErrorCode(17, `Name is already used by ${defaultMsg}.`);
      });

      it('tests ecp/delete messages', () => {
        finishedTask.name = 'ecp/delete';
        testDelete(defaultMsg);
      });
    });

    describe('crush rule tasks', () => {
      beforeEach(() => {
        const metadata = {
          name: 'someRuleName'
        };
        defaultMsg = `crush rule '${metadata.name}'`;
        finishedTask.metadata = metadata;
      });

      it('tests crushRule/create messages', () => {
        finishedTask.name = 'crushRule/create';
        testCreate(defaultMsg);
        testErrorCode(17, `Name is already used by ${defaultMsg}.`);
      });

      it('tests crushRule/delete messages', () => {
        finishedTask.name = 'crushRule/delete';
        testDelete(defaultMsg);
      });
    });

    describe('rbd tasks', () => {
      let metadata: Record<string, any>;
      let childMsg: string;
      let destinationMsg: string;
      let snapMsg: string;

      beforeEach(() => {
        metadata = {
          pool_name: 'somePool',
          image_name: 'someImage',
          image_id: '12345',
          image_spec: 'somePool/someImage',
          image_id_spec: 'somePool/12345',
          snapshot_name: 'someSnapShot',
          dest_pool_name: 'someDestinationPool',
          dest_image_name: 'someDestinationImage',
          child_pool_name: 'someChildPool',
          child_image_name: 'someChildImage',
          new_image_name: 'someImage2'
        };
        defaultMsg = `RBD '${metadata.pool_name}/${metadata.image_name}'`;
        childMsg = `RBD '${metadata.child_pool_name}/${metadata.child_image_name}'`;
        destinationMsg = `RBD '${metadata.dest_pool_name}/${metadata.dest_image_name}'`;
        snapMsg = `RBD snapshot '${metadata.pool_name}/${metadata.image_name}@${metadata.snapshot_name}'`;
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
        testErrorCode(16, `${defaultMsg} is busy.`);
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

      it('tests rbd/trash/move messages', () => {
        finishedTask.name = 'rbd/trash/move';
        testMessages(
          new TaskMessageOperation('Moving', 'move', 'Moved'),
          `image '${metadata.image_spec}' to trash`
        );
        testErrorCode(2, `Could not find image.`);
      });

      it('tests rbd/trash/restore messages', () => {
        finishedTask.name = 'rbd/trash/restore';
        testMessages(
          new TaskMessageOperation('Restoring', 'restore', 'Restored'),
          `image '${metadata.image_id_spec}' ` + `into '${metadata.new_image_name}'`
        );
        testErrorCode(17, `Image name '${metadata.new_image_name}' is already in use.`);
      });

      it('tests rbd/trash/remove messages', () => {
        finishedTask.name = 'rbd/trash/remove';
        testDelete(`image '${metadata.image_id_spec}'`);
      });

      it('tests rbd/trash/purge messages', () => {
        finishedTask.name = 'rbd/trash/purge';
        testMessages(
          new TaskMessageOperation('Purging', 'purge', 'Purged'),
          `images from '${metadata.pool_name}'`
        );
      });
    });
    describe('rbd tasks', () => {
      let metadata;
      let modeMsg: string;
      let peerMsg: string;

      beforeEach(() => {
        metadata = {
          pool_name: 'somePool'
        };
        modeMsg = `mirror mode for pool '${metadata.pool_name}'`;
        peerMsg = `mirror peer for pool '${metadata.pool_name}'`;
        finishedTask.metadata = metadata;
      });
      it('tests rbd/mirroring/site_name/edit messages', () => {
        finishedTask.name = 'rbd/mirroring/site_name/edit';
        testUpdate('mirroring site name');
      });
      it('tests rbd/mirroring/bootstrap/create messages', () => {
        finishedTask.name = 'rbd/mirroring/bootstrap/create';
        testCreate('bootstrap token');
      });
      it('tests rbd/mirroring/bootstrap/import messages', () => {
        finishedTask.name = 'rbd/mirroring/bootstrap/import';
        testImport('bootstrap token');
      });
      it('tests rbd/mirroring/pool/edit messages', () => {
        finishedTask.name = 'rbd/mirroring/pool/edit';
        testUpdate(modeMsg);
        testErrorCode(16, 'Cannot disable mirroring because it contains a peer.');
      });
      it('tests rbd/mirroring/peer/edit messages', () => {
        finishedTask.name = 'rbd/mirroring/peer/edit';
        testUpdate(peerMsg);
      });
      it('tests rbd/mirroring/peer/add messages', () => {
        finishedTask.name = 'rbd/mirroring/peer/add';
        testCreate(peerMsg);
      });
      it('tests rbd/mirroring/peer/delete messages', () => {
        finishedTask.name = 'rbd/mirroring/peer/delete';
        testDelete(peerMsg);
      });
    });
  });
});
