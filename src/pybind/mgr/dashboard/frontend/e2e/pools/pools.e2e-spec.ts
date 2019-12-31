import { PoolPageHelper } from './pools.po';

describe('Pools page', () => {
  let pools: PoolPageHelper;
  const poolName = 'pool_e2e_pool/test';

  beforeAll(async () => {
    pools = new PoolPageHelper();
    await pools.navigateTo();
  });

  afterEach(async () => {
    await PoolPageHelper.checkConsole();
  });

  describe('breadcrumb and tab tests', () => {
    it('should open and show breadcrumb', async () => {
      await pools.waitTextToBePresent(pools.getBreadcrumb(), 'Pools');
    });

    it('should show two tabs', async () => {
      await expect(pools.getTabsCount()).toEqual(2);
    });

    it('should show pools list tab at first', async () => {
      await expect(pools.getTabText(0)).toEqual('Pools List');
    });

    it('should show overall performance as a second tab', async () => {
      await expect(pools.getTabText(1)).toEqual('Overall Performance');
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
    await pools.navigateTo();
    await pools.delete(poolName);
  });
});
