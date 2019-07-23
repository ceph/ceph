import { Helper } from '../helper.po';

describe('Pools page', () => {
  let pools;
  const poolName = 'pool_e2e_pool_test';

  beforeAll(() => {
    pools = new Helper().pools;
    pools.navigateTo();
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb and tab tests', () => {
    it('should open and show breadcrumb', () => {
      expect(pools.getBreadcrumbText()).toEqual('Pools');
    });

    it('should show two tabs', () => {
      expect(pools.getTabsCount()).toEqual(2);
    });

    it('should show pools list tab at first', () => {
      expect(pools.getTabText(0)).toEqual('Pools List');
    });

    it('should show overall performance as a second tab', () => {
      expect(pools.getTabText(1)).toEqual('Overall Performance');
    });
  });

  it('should create a pool', () => {
    pools.exist(poolName, false).then(() => {
      pools.navigateTo('create');
      pools.create(poolName, 8).then(() => {
        pools.navigateTo();
        pools.exist(poolName, true);
      });
    });
  });

  it('should delete a pool', () => {
    pools.exist(poolName);
    pools.delete(poolName).then(() => {
      pools.navigateTo();
      pools.exist(poolName, false);
    });
  });
});
