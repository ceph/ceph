import { PoolPageHelper } from '../pools/pools.po';
import { MirroringPageHelper } from './mirroring.po';

describe('Mirroring page', () => {
  const pools = new PoolPageHelper();
  const mirroring = new MirroringPageHelper();

  beforeEach(() => {
    cy.login();
    mirroring.navigateTo();
  });

  it('should open and show breadcrumb', () => {
    mirroring.expectBreadcrumbText('Mirroring');
  });

  it('should show three tabs', () => {
    mirroring.getTabsCount().should('eq', 3);
  });

  it('should show text for all tabs', () => {
    mirroring.getTabText(0).should('eq', 'Issues');
    mirroring.getTabText(1).should('eq', 'Syncing');
    mirroring.getTabText(2).should('eq', 'Ready');
  });

  describe('checks that edit mode functionality shows in the pools table', () => {
    const poolName = 'mirroring_test';

    beforeEach(() => {
      pools.navigateTo('create'); // Need pool for mirroring testing
      pools.create(poolName, 8, 'rbd');
      pools.navigateTo();
      pools.exist(poolName, true);
    });

    it('tests editing mode for pools', () => {
      mirroring.navigateTo();

      mirroring.editMirror(poolName, 'Pool');
      mirroring.getFirstTableCell('pool').should('be.visible');
      mirroring.editMirror(poolName, 'Image');
      mirroring.getFirstTableCell('image').should('be.visible');
      mirroring.editMirror(poolName, 'Disabled');
      mirroring.getFirstTableCell('disabled').should('be.visible');
    });

    afterEach(() => {
      pools.navigateTo();
      pools.delete(poolName);
    });
  });
});
