import { Helper } from '../helper.po';

describe('OSDs page', () => {
  let osds: Helper['osds'];

  beforeAll(() => {
    osds = new Helper().osds;
  });

  afterEach(async () => {
    await Helper.checkConsole();
  });

  describe('breadcrumb and tab tests', () => {
    beforeAll(async () => {
      await osds.navigateTo();
    });

    it('should open and show breadcrumb', async () => {
      expect(await osds.getBreadcrumbText()).toEqual('OSDs');
    });

    it('should show two tabs', async () => {
      expect(await osds.getTabsCount()).toEqual(2);
    });

    it('should show OSDs list tab at first', async () => {
      expect(await osds.getTabText(0)).toEqual('OSDs List');
    });

    it('should show overall performance as a second tab', async () => {
      expect(await osds.getTabText(1)).toEqual('Overall Performance');
    });
  });
});
