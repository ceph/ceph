import { PoolPageHelper } from '../pools/pools.po';
import { LogsPageHelper } from './logs.po';

describe('Logs page', () => {
  const logs = new LogsPageHelper();
  const pools = new PoolPageHelper();

  const poolname = 'e2e_logs_test_pool';
  const today = new Date();
  let hour = today.getHours();
  if (hour > 12) {
    hour = hour - 12;
  }
  const minute = today.getMinutes();

  beforeEach(() => {
    cy.login();
  });

  describe('breadcrumb and tab tests', () => {
    beforeEach(() => {
      logs.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      logs.expectBreadcrumbText('Logs');
    });

    it('should show three tabs', () => {
      logs.getTabsCount().should('eq', 3);
    });

    it('should show cluster logs tab at first', () => {
      logs.getTabText(0).should('eq', 'Cluster Logs');
    });

    it('should show audit logs as a second tab', () => {
      logs.getTabText(1).should('eq', 'Audit Logs');
    });

    it('should show daemon logs as a third tab', () => {
      logs.getTabText(2).should('eq', 'Daemon Logs');
    });
  });

  describe('audit logs respond to pool creation and deletion test', () => {
    it('should create pool and check audit logs reacted', () => {
      pools.navigateTo('create');
      pools.create(poolname, 8, ['rbd']);
      pools.navigateTo();
      pools.existTableCell(poolname, true);
      logs.checkAuditForPoolFunction(poolname, 'create', hour, minute);
    });

    it('should delete pool and check audit logs reacted', () => {
      pools.navigateTo();
      pools.delete(poolname);
      logs.checkAuditForPoolFunction(poolname, 'delete', hour, minute);
    });
  });
});
