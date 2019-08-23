import { Helper } from '../helper.po';
import { PoolPageHelper } from './pools.po';

describe('Pools page', () => {
  let pools: PoolPageHelper;
  const poolName = 'pool_e2e_pool_test';

  beforeAll(async () => {
    pools = new PoolPageHelper();
    await pools.navigateTo();
  });

  afterEach(async () => {
    await Helper.checkConsole();
  });

  describe('breadcrumb and tab tests', () => {
    it('should open and show breadcrumb', async () => {
      expect(await pools.getBreadcrumbText()).toEqual('Pools');
    });

    it('should show two tabs', async () => {
      expect(await pools.getTabsCount()).toEqual(2);
    });

    it('should show pools list tab at first', async () => {
      expect(await pools.getTabText(0)).toEqual('Pools List');
    });

    it('should show overall performance as a second tab', async () => {
      expect(await pools.getTabText(1)).toEqual('Overall Performance');
    });
  });

  it('should create a pool', async () => {
    await pools.exist(poolName, false);
    await pools.navigateTo('create');
    await pools.create(poolName, 8);
    await pools.navigateTo();
    await pools.exist(poolName, true);
  });

  it('should edit a pools placement group', async () => {
    await pools.exist(poolName, true);
    await pools.navigateTo();
    await pools.edit_pool_pg(poolName, 32);
  });

  it('should delete a pool', async () => {
    await pools.exist(poolName);
    await pools.delete(poolName);
    await pools.navigateTo();
    await pools.exist(poolName, false);
  });
});
