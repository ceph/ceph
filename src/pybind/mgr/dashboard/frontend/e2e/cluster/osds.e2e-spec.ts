import { OSDsPageHelper } from './osds.po';

describe('OSDs page', () => {
  let osds: OSDsPageHelper;

  beforeAll(() => {
    osds = new OSDsPageHelper();
  });

  afterEach(async () => {
    await OSDsPageHelper.checkConsole();
  });

  describe('breadcrumb and tab tests', () => {
    beforeAll(async () => {
      await osds.navigateTo();
    });

    it('should open and show breadcrumb', async () => {
      await osds.waitTextToBePresent(osds.getBreadcrumb(), 'OSDs');
    });

    it('should show two tabs', async () => {
      await expect(osds.getTabsCount()).toEqual(2);
    });

    it('should show OSDs list tab at first', async () => {
      await expect(osds.getTabText(0)).toEqual('OSDs List');
    });

    it('should show overall performance as a second tab', async () => {
      await expect(osds.getTabText(1)).toEqual('Overall Performance');
    });
  });
});
