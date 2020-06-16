import { Helper } from '../helper.po';
import { PoolsPage } from './pools.po';

describe('Pools page', () => {
  let page: PoolsPage;

  beforeAll(() => {
    page = new PoolsPage();
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb and tab tests', () => {
    beforeAll(() => {
      page.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      Helper.waitTextToBePresent(Helper.getBreadcrumb(), 'Pools');
    });

    it('should show two tabs', () => {
      expect(Helper.getTabsCount()).toEqual(2);
    });

    it('should show pools list tab at first', () => {
      expect(Helper.getTabText(0)).toEqual('Pools List');
    });

    it('should show overall performance as a second tab', () => {
      expect(Helper.getTabText(1)).toEqual('Overall Performance');
    });
  });
});
