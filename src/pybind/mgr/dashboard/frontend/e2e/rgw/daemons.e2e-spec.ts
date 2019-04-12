import { Helper } from '../helper.po';
import { DaemonsPage } from './daemons.po';

describe('RGW daemons page', () => {
  let page: DaemonsPage;

  beforeAll(() => {
    page = new DaemonsPage();
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb and tab tests', () => {
    beforeAll(() => {
      page.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(Helper.getBreadcrumbText()).toEqual('Daemons');
    });

    it('should show two tabs', () => {
      expect(Helper.getTabsCount()).toEqual(2);
    });

    it('should show daemons list tab at first', () => {
      expect(Helper.getTabText(0)).toEqual('Daemons List');
    });

    it('should show overall performance as a second tab', () => {
      expect(Helper.getTabText(1)).toEqual('Overall Performance');
    });
  });
});
