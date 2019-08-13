import { Helper } from '../helper.po';

describe('Manager modules page', () => {
  let mgrmodules: Helper['mgrmodules'];

  beforeAll(() => {
    mgrmodules = new Helper().mgrmodules;
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(() => {
      mgrmodules.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(mgrmodules.getBreadcrumbText()).toEqual('Manager modules');
    });
  });

  describe('verifies editing functionality for manager modules', () => {
    beforeAll(() => {
      mgrmodules.navigateTo();
    });

    it('should test editing on ansible module', () => {
      const ansibleArr = [['rq', 'ca_bundle'], ['colts', 'server_location']];
      mgrmodules.editMgrModule('ansible', ansibleArr);
    });

    it('should test editing on deepsea module', () => {
      const deepseaArr = [
        ['rq', 'salt_api_eauth'],
        ['alm', 'salt_api_password'],
        ['bu', 'salt_api_url'],
        ['sox', 'salt_api_username']
      ];
      mgrmodules.editMgrModule('deepsea', deepseaArr);
    });

    it('should test editing on diskprediction_local module', () => {
      const diskpredLocalArr = [['11', 'predict_interval'], ['0122', 'sleep_interval']];
      mgrmodules.editMgrModule('diskprediction_local', diskpredLocalArr);
    });

    it('should test editing on balancer module', () => {
      const balancerArr = [['rq', 'pool_ids']];
      mgrmodules.editMgrModule('balancer', balancerArr);
    });

    it('should test editing on dashboard module', () => {
      const dashboardArr = [['rq', 'AUDIT_API_ENABLED'], ['rafa', 'GRAFANA_API_PASSWORD']];
      mgrmodules.editMgrModule('dashboard', dashboardArr);
    });

    it('should test editing on devicehealth module', () => {
      mgrmodules.editDevicehealth('1987', 'sox', '1999', '2020', '456', '567');
    });
  });
});
