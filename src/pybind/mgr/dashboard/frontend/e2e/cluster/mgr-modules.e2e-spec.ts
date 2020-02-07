import { ManagerModulesPageHelper } from './mgr-modules.po';

describe('Manager modules page', () => {
  let mgrmodules: ManagerModulesPageHelper;

  beforeAll(() => {
    mgrmodules = new ManagerModulesPageHelper();
  });

  afterEach(async () => {
    await ManagerModulesPageHelper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(async () => {
      await mgrmodules.navigateTo();
    });

    it('should open and show breadcrumb', async () => {
      await mgrmodules.waitTextToBePresent(mgrmodules.getBreadcrumb(), 'Manager modules');
    });
  });

  describe('verifies editing functionality for manager modules', () => {
    beforeAll(async () => {
      await mgrmodules.navigateTo();
    });

    it('should test editing on diskprediction_local module', async () => {
      const diskpredLocalArr = [['11', 'predict_interval'], ['0122', 'sleep_interval']];
      await mgrmodules.editMgrModule('diskprediction_local', diskpredLocalArr);
    });

    it('should test editing on balancer module', async () => {
      const balancerArr = [['rq', 'pool_ids']];
      await mgrmodules.editMgrModule('balancer', balancerArr);
    });

    it('should test editing on dashboard module', async () => {
      const dashboardArr = [['rq', 'RGW_API_USER_ID'], ['rafa', 'GRAFANA_API_PASSWORD']];
      await mgrmodules.editMgrModule('dashboard', dashboardArr);
    });

    it('should test editing on devicehealth module', async () => {
      await mgrmodules.editDevicehealth('1987', 'sox', '1999', '2020', '456', '567');
    });
  });
});
