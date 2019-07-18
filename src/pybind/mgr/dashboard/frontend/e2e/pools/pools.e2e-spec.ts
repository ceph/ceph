import { Helper } from '../helper.po';
import { PoolPageHelper } from './pools.po';

describe('Pools page', () => {
  let page: PoolPageHelper;
  let helper: Helper;
  const poolName = 'pool_e2e_pool_test';

  beforeAll(() => {
    page = new PoolPageHelper();
    helper = new Helper();
    page.navigateTo();
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb and tab tests', () => {
    it('should open and show breadcrumb', () => {
      expect(PoolPageHelper.getBreadcrumbText()).toEqual('Pools');
    });

    it('should show two tabs', () => {
      expect(PoolPageHelper.getTabsCount()).toEqual(2);
    });

    it('should show pools list tab at first', () => {
      expect(PoolPageHelper.getTabText(0)).toEqual('Pools List');
    });

    it('should show overall performance as a second tab', () => {
      expect(PoolPageHelper.getTabText(1)).toEqual('Overall Performance');
    });
  });

  it('should create a pool', () => {
    helper.pools.exist(poolName, false).then(() => {
      helper.pools.navigateTo('create');
      helper.pools.create(poolName, 8).then(() => {
        helper.pools.navigateTo();
        helper.pools.exist(poolName, true);
      });
    });
  });

  it('should delete a pool', () => {
    helper.pools.exist(poolName);
    helper.pools.delete(poolName).then(() => {
      helper.pools.navigateTo();
      helper.pools.exist(poolName, false);
    });
  });
});
