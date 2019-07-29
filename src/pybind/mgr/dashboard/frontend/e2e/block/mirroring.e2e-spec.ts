import { Helper } from '../helper.po';

describe('Mirroring page', () => {
  let mirroring: Helper['mirroring'];
  let pools: Helper['pools'];

  beforeAll(() => {
    mirroring = new Helper().mirroring;
    pools = new Helper().pools;
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb and tab tests', () => {
    beforeAll(() => {
      mirroring.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(mirroring.getBreadcrumbText()).toEqual('Mirroring');
    });

    it('should show three tabs', () => {
      expect(mirroring.getTabsCount()).toEqual(3);
    });

    it('should show text for all tabs', () => {
      expect(mirroring.getTabText(0)).toEqual('Issues');
      expect(mirroring.getTabText(1)).toEqual('Syncing');
      expect(mirroring.getTabText(2)).toEqual('Ready');
    });
  });

  describe('checks that edit mode functionality shows in the pools table', () => {
    const poolName = 'mirrorpoolrq';

    beforeAll(() => {
      pools.navigateTo('create'); // Need pool for mirroring testing
      pools.create(poolName, 8, 'rbd').then(() => {
        pools.navigateTo();
        pools.exist(poolName, true);
      });
    });

    it('tests editing mode for pools', () => {
      mirroring.navigateTo();
      expect(mirroring.editMirror(poolName, 'Pool'));
      expect(mirroring.getFirstTableCellWithText('pool').isPresent()).toBe(true);
      expect(mirroring.editMirror(poolName, 'Image'));
      expect(mirroring.getFirstTableCellWithText('image').isPresent()).toBe(true);
      expect(mirroring.editMirror(poolName, 'Disabled'));
      expect(mirroring.getFirstTableCellWithText('disabled').isPresent()).toBe(true);
    });

    afterAll(() => {
      pools.navigateTo(); // Deletes mirroring test pool
      pools.delete(poolName).then(() => {
        pools.navigateTo();
        pools.exist(poolName, false);
      });
    });
  });
});
