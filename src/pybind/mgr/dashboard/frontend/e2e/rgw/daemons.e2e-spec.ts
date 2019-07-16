import { Helper } from '../helper.po';

describe('RGW daemons page', () => {
  let daemons;

  beforeAll(() => {
    daemons = new Helper().daemons;
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb and tab tests', () => {
    beforeAll(() => {
      daemons.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(daemons.getBreadcrumbText()).toEqual('Daemons');
    });

    it('should show two tabs', () => {
      expect(daemons.getTabsCount()).toEqual(2);
    });

    it('should show daemons list tab at first', () => {
      expect(daemons.getTabText(0)).toEqual('Daemons List');
    });

    it('should show overall performance as a second tab', () => {
      expect(daemons.getTabText(1)).toEqual('Overall Performance');
    });
  });
});
