import { Helper } from '../helper.po';

describe('Logs page', () => {
  let logs: Helper['logs'];
  let pools: Helper['pools'];
  let configuration: Helper['configuration'];

  const poolname = 'logs_e2e_test_pool';
  const configname = 'log_graylog_port';
  const today = new Date();
  let hour = today.getHours();
  if (hour > 12) {
    hour = hour - 12;
  }
  const minute = today.getMinutes();

  beforeAll(() => {
    logs = new Helper().logs;
    pools = new Helper().pools;
    configuration = new Helper().configuration;
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb and tab tests', () => {
    beforeAll(() => {
      logs.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(logs.getBreadcrumbText()).toEqual('Logs');
    });

    it('should show two tabs', () => {
      expect(logs.getTabsCount()).toEqual(2);
    });

    it('should show cluster logs tab at first', () => {
      expect(logs.getTabText(0)).toEqual('Cluster Logs');
    });

    it('should show audit logs as a second tab', () => {
      expect(logs.getTabText(1)).toEqual('Audit Logs');
    });
  });

  describe('audit logs respond to pool creation and deletion test', () => {
    it('should create pool and check audit logs reacted', () => {
      pools.navigateTo('create');
      pools.create(poolname, 8);

      pools.navigateTo();
      pools.exist(poolname, true);

      logs.navigateTo();
      logs.checkAuditForPoolFunction(poolname, 'create', hour, minute);
    });

    it('should delete pool and check audit logs reacted', () => {
      pools.navigateTo();
      pools.delete(poolname);

      pools.navigateTo();
      pools.exist(poolname, false);

      logs.navigateTo();
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
