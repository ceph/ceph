import { PoolPageHelper } from '../pools/pools.po';
import { MirroringPageHelper } from './mirroring.po';

describe('Mirroring page', () => {
  let pools: PoolPageHelper;
  let mirroring: MirroringPageHelper;

  beforeAll(() => {
    mirroring = new MirroringPageHelper();
    pools = new PoolPageHelper();
  });

  afterEach(async () => {
    await MirroringPageHelper.checkConsole();
  });

  describe('breadcrumb and tab tests', () => {
    beforeAll(async () => {
      await mirroring.navigateTo();
    });

    it('should open and show breadcrumb', async () => {
      await mirroring.waitTextToBePresent(mirroring.getBreadcrumb(), 'Mirroring');
    });

    it('should show three tabs', async () => {
      await expect(mirroring.getTabsCount()).toEqual(3);
    });

    it('should show text for all tabs', async () => {
      await expect(mirroring.getTabText(0)).toEqual('Issues');
      await expect(mirroring.getTabText(1)).toEqual('Syncing');
      await expect(mirroring.getTabText(2)).toEqual('Ready');
    });
  });

  describe('checks that edit mode functionality shows in the pools table', () => {
    const poolName = 'mirroring_test';

    beforeAll(async () => {
      await pools.navigateTo('create'); // Need pool for mirroring testing
      await pools.create(poolName, 8, 'rbd');
      await pools.navigateTo();
      await pools.exist(poolName, true);
    });

    it('tests editing mode for pools', async () => {
      await mirroring.navigateTo();

      await mirroring.editMirror(poolName, 'Pool');
      await expect(mirroring.getFirstTableCellWithText('pool').isPresent()).toBe(true);
      await mirroring.editMirror(poolName, 'Image');
      await expect(mirroring.getFirstTableCellWithText('image').isPresent()).toBe(true);
      await mirroring.editMirror(poolName, 'Disabled');
      await expect(mirroring.getFirstTableCellWithText('disabled').isPresent()).toBe(true);
    });

    afterAll(async () => {
      await pools.navigateTo();
      await pools.delete(poolName);
    });
  });
});
