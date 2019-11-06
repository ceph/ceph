import { PoolPageHelper } from '../pools/pools.po';
import { ConfigurationPageHelper } from './configuration.po';
import { LogsPageHelper } from './logs.po';

describe('Logs page', () => {
  let logs: LogsPageHelper;
  let pools: PoolPageHelper;
  let configuration: ConfigurationPageHelper;

  const poolname = 'logs_e2e_test_pool';
  const configname = 'log_graylog_port';
  const today = new Date();
  let hour = today.getHours();
  if (hour > 12) {
    hour = hour - 12;
  }
  const minute = today.getMinutes();

  beforeAll(() => {
    logs = new LogsPageHelper();
    pools = new PoolPageHelper();
    configuration = new ConfigurationPageHelper();
  });

  afterEach(async () => {
    await LogsPageHelper.checkConsole();
  });

  describe('breadcrumb and tab tests', () => {
    beforeAll(async () => {
      await logs.navigateTo();
    });

    it('should open and show breadcrumb', async () => {
      await logs.waitTextToBePresent(logs.getBreadcrumb(), 'Logs');
    });

    it('should show two tabs', async () => {
      await expect(logs.getTabsCount()).toEqual(2);
    });

    it('should show cluster logs tab at first', async () => {
      await expect(logs.getTabText(0)).toEqual('Cluster Logs');
    });

    it('should show audit logs as a second tab', async () => {
      await expect(logs.getTabText(1)).toEqual('Audit Logs');
    });
  });

  describe('audit logs respond to pool creation and deletion test', () => {
    it('should create pool and check audit logs reacted', async () => {
      await pools.navigateTo('create');
      await pools.create(poolname, 8);

      await pools.navigateTo();
      await pools.exist(poolname, true);

      await logs.checkAuditForPoolFunction(poolname, 'create', hour, minute);
    });

    it('should delete pool and check audit logs reacted', async () => {
      await pools.navigateTo();
      await pools.delete(poolname);

      await logs.navigateTo();
      await logs.checkAuditForPoolFunction(poolname, 'delete', hour, minute);
    });
  });

  describe('audit logs respond to editing configuration setting test', () => {
    it('should change config settings and check audit logs reacted', async () => {
      await configuration.navigateTo();
      await configuration.edit(configname, ['global', '5']);

      await logs.navigateTo();
      await logs.checkAuditForConfigChange(configname, 'global', hour, minute);

      await configuration.navigateTo();
      await configuration.configClear(configname);
    });
  });
});
