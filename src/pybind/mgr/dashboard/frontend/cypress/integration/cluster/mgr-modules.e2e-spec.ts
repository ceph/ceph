import { ManagerModulesPageHelper } from './mgr-modules.po';

describe('Manager modules page', () => {
  const mgrmodules = new ManagerModulesPageHelper();

  beforeEach(() => {
    cy.login();
    mgrmodules.navigateTo();
  });

  describe('breadcrumb test', () => {
    it('should open and show breadcrumb', () => {
      mgrmodules.expectBreadcrumbText('Manager modules');
    });
  });

  describe('verifies editing functionality for manager modules', () => {
    it('should test editing on diskprediction_local module', () => {
      const diskpredLocalArr = [
        ['11', 'predict_interval'],
        ['0122', 'sleep_interval']
      ];
      mgrmodules.editMgrModule('diskprediction_local', diskpredLocalArr);
    });

    it('should test editing on balancer module', () => {
      const balancerArr = [['rq', 'pool_ids']];
      mgrmodules.editMgrModule('balancer', balancerArr);
    });

    it('should test editing on dashboard module', () => {
      const dashboardArr = [
        ['rq', 'RGW_API_USER_ID'],
        ['rafa', 'GRAFANA_API_PASSWORD']
      ];
      mgrmodules.editMgrModule('dashboard', dashboardArr);
    });

    it('should test editing on devicehealth module', () => {
      mgrmodules.editDevicehealth('1987', 'sox', '1999', '2020', '456', '567');
    });
  });
});
