import { PoolPageHelper } from '../pools/pools.po';
import { ConfigurationPageHelper } from './configuration.po';
import { LogsPageHelper } from './logs.po';

describe('Logs page', () => {
  const logs = new LogsPageHelper();
  const pools = new PoolPageHelper();
  const configuration = new ConfigurationPageHelper();

  const poolname = 'e2e_logs_test_pool';
  const configname = 'log_graylog_port';
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

    it('should show two tabs', () => {
      logs.getTabsCount().should('eq', 2);
    });

    it('should show cluster logs tab at first', () => {
      logs.getTabText(0).should('eq', 'Cluster Logs');
    });

    it('should show audit logs as a second tab', () => {
      logs.getTabText(1).should('eq', 'Audit Logs');
    });
  });

  describe('audit logs respond to pool creation and deletion test', () => {
    it('should create pool and check audit logs reacted', () => {
      pools.navigateTo('create');
      pools.create(poolname, 8);
      pools.navigateTo();
      pools.exist(poolname, true);
      logs.checkAuditForPoolFunction(poolname, 'create', hour, minute);
    });

    it('should delete pool and check audit logs reacted', () => {
      pools.navigateTo();
      pools.delete(poolname);
      logs.checkAuditForPoolFunction(poolname, 'delete', hour, minute);
    });
  });

  describe('audit logs respond to editing configuration setting test', () => {
    it('should change config settings and check audit logs reacted', () => {
      configuration.navigateTo();
      configuration.edit(configname, ['global', '5']);

      logs.navigateTo();
      logs.checkAuditForConfigChange(configname, 'global', hour, minute);

      configuration.navigateTo();
      configuration.configClear(configname);
    });
  });
});
