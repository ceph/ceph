import { PoolPageHelper } from './pools.po';

describe('Pools page', () => {
  const pools = new PoolPageHelper();
  const poolName = 'pool_e2e_pool/test';

  beforeEach(() => {
    cy.login();
    pools.navigateTo();
  });

  describe('breadcrumb and tab tests', () => {
    it('should open and show breadcrumb', () => {
      pools.expectBreadcrumbText('Pools');
    });

    it('should show two tabs', () => {
      pools.getTabsCount().should('equal', 2);
    });

    it('should show pools list tab at first', () => {
      pools.getTabText(0).should('eq', 'Pools List');
    });

    it('should show overall performance as a second tab', () => {
      pools.getTabText(1).should('eq', 'Overall Performance');
    });
  });

  describe('Create, update and destroy', () => {
    it('should create a pool', () => {
      pools.exist(poolName, false);
      pools.navigateTo('create');
      pools.create(poolName, 8);
      pools.exist(poolName, true);
    });

    it('should edit a pools placement group', () => {
      pools.exist(poolName, true);
      pools.edit_pool_pg(poolName, 32);
    });

    it('should delete a pool', () => {
      pools.delete(poolName);
    });
  });
});
