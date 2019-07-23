import { Helper } from '../helper.po';

describe('OSDs page', () => {
  let osds;

  beforeAll(() => {
    osds = new Helper().osds;
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb and tab tests', () => {
    beforeAll(() => {
      osds.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(osds.getBreadcrumbText()).toEqual('OSDs');
    });

    it('should show two tabs', () => {
      expect(osds.getTabsCount()).toEqual(2);
    });

    it('should show OSDs list tab at first', () => {
      expect(osds.getTabText(0)).toEqual('OSDs List');
    });

    it('should show overall performance as a second tab', () => {
      expect(osds.getTabText(1)).toEqual('Overall Performance');
    });
  });
});
