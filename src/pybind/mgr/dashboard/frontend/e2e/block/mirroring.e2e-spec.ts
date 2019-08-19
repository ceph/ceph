import { Helper } from '../helper.po';

describe('Mirroring page', () => {
  let mirroring: Helper['mirroring'];
  let pools: Helper['pools'];

  beforeAll(() => {
    mirroring = new Helper().mirroring;
    pools = new Helper().pools;
  });

  afterEach(async () => {
    await Helper.checkConsole();
  });

  describe('breadcrumb and tab tests', () => {
    beforeAll(async () => {
      await mirroring.navigateTo();
    });

    it('should open and show breadcrumb', async () => {
      expect(await mirroring.getBreadcrumbText()).toEqual('Mirroring');
    });

    it('should show three tabs', async () => {
      expect(await mirroring.getTabsCount()).toEqual(3);
    });

    it('should show text for all tabs', async () => {
      expect(await mirroring.getTabText(0)).toEqual('Issues');
      expect(await mirroring.getTabText(1)).toEqual('Syncing');
      expect(await mirroring.getTabText(2)).toEqual('Ready');
    });
  });

  describe('checks that edit mode functionality shows in the pools table', async () => {
    const poolName = 'mirrorpoolrq';

    beforeAll(async () => {
      await pools.navigateTo('create'); // Need pool for mirroring testing
      await pools.create(poolName, 8, 'rbd');
      // console.log(`before second navigateTo()`);
      await pools.navigateTo();
      // console.log(`before pools.exist(${poolName})`);
      await pools.exist(poolName, true);
      // console.log(`beforeAll done`);
    });

    it('tests editing mode for pools', async () => {
      await mirroring.navigateTo();
      expect(await mirroring.editMirror(poolName, 'Pool'));
      expect(await mirroring.getFirstTableCellWithText('pool').isPresent()).toBe(true);
      expect(await mirroring.editMirror(poolName, 'Image'));
      expect(await mirroring.getFirstTableCellWithText('image').isPresent()).toBe(true);
      expect(await mirroring.editMirror(poolName, 'Disabled'));
      expect(await mirroring.getFirstTableCellWithText('disabled').isPresent()).toBe(true);
    });

    afterAll(async () => {
      await pools.navigateTo(); // Deletes mirroring test pool
      await pools.delete(poolName);
      await pools.navigateTo();
      await pools.exist(poolName, false);
    });
  });
});
