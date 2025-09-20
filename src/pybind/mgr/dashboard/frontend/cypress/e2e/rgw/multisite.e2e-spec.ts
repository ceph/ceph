import { BucketsPageHelper } from './buckets.po';
import { MultisitePageHelper } from './multisite.po';

describe('Multisite page', () => {
  const multisite = new MultisitePageHelper();
  const buckets = new BucketsPageHelper();
  const bucket_name = 'e2ebucket';

  before(() => {
    cy.login();
    buckets.navigateTo('create');
    buckets.create(bucket_name, BucketsPageHelper.USERS[0]);
    buckets.getFirstTableCell(bucket_name).should('exist');
  });

  beforeEach(() => {
    cy.login();
  });

  describe('table tests', () => {
    it('should show table on sync-policy page', () => {
      multisite.navigateTo();
      multisite.tableExist();
    });
  });

  describe('create, edit & delete sync group policy', () => {
    it('should create policy', () => {
      multisite.navigateTo('create');
      multisite.create('test', 'Enabled', bucket_name);
      multisite.getFirstTableCell('test').should('exist');
    });

    it('should edit policy status', () => {
      multisite.navigateTo();
      multisite.edit('test', 'Forbidden', bucket_name);
    });

    it('should delete policy', () => {
      multisite.navigateTo();
      multisite.delete('test', null, null, true, true);
    });
  });

  // @TODO: <skipping tests as need to setup multisite configuration to test flow and pipe>
  describe.skip('create, edit & delete symmetrical sync Flow', () => {
    it('Preparing...(creating sync group policy)', () => {
      multisite.navigateTo('create');
      multisite.create('test', 'Enabled', bucket_name);
      multisite.getFirstTableCell('test').should('exist');
    });
    describe('symmetrical Flow creation started', () => {
      beforeEach(() => {
        multisite.navigateTo();
        multisite.getExpandCollapseElement().click();
      });

      it('should create flow', () => {
        multisite.createSymmetricalFlow('new-sym-flow', ['zone1-zg1-realm1']);
      });

      it('should modify flow zones', () => {
        multisite.editSymFlow('new-sym-flow', 'zone2-zg1-realm1');
      });

      it('should delete flow', () => {
        multisite.deleteSymFlow('new-sym-flow');
      });
    });
  });

  describe.skip('create, edit & delete directional sync Flow', () => {
    beforeEach(() => {
      multisite.navigateTo();
      multisite.getExpandCollapseElement().click();
    });

    it('should create flow', () => {
      multisite.createDirectionalFlow(
        'new-dir-flow',
        ['zone1-zg1-realm1', 'zone2-zg1-realm1'],
        ['new-zone']
      );
    });
  });

  describe.skip('create, edit, delete pipe', () => {
    beforeEach(() => {
      multisite.navigateTo();
      multisite.getExpandCollapseElement().click();
    });

    it('should create pipe', () => {
      multisite.createPipe('new-pipe', ['zone1-zg1-realm1'], ['zone3-zg2-realm1']);
    });

    it('should modify pipe zones', () => {
      multisite.editPipe('new-pipe', 'zone2-zg1-realm1');
    });

    it('should delete pipe', () => {
      multisite.deletePipe('new-pipe');
    });
  });

  describe('Multi-site topology viewer', () => {
    it('should show topology viewer', () => {
      multisite.navigateTo('topology');
      multisite.topologyViewerExist();
    });

    describe('Multisite replication wizard', () => {
      beforeEach(() => {
        multisite.navigateTo('wizard');
      });

      it('should show replication wizard', () => {
        multisite.replicationWizardExist();
      });

      it('should verify the wizard is properly loaded', () => {
        multisite.replicationWizardExist();
        // // Verify first step
        multisite.verifyWizardContents('CreateRealmZonegroup');
        // Verify second step
        multisite.verifyWizardContents('CreateZone');
        // Verify the review
        multisite.verifyWizardContents('Review');
      });
    });
  });
});
