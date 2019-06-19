import { Helper } from '../helper.po';
import { PoolPageHelper } from './pools.po';

describe('Pools page', () => {
  let page: PoolPageHelper;

  beforeAll(() => {
    page = new PoolPageHelper();
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb and tab tests', () => {
    beforeAll(() => {
      page.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(PoolPageHelper.getBreadcrumbText()).toEqual('Pools');
    });

    it('should show two tabs', () => {
      expect(PoolPageHelper.getTabsCount()).toEqual(2);
    });

    it('should show pools list tab at first', () => {
      expect(PoolPageHelper.getTabText(0)).toEqual('Pools List');
    });

    it('should show overall performance as a second tab', () => {
      expect(PoolPageHelper.getTabText(1)).toEqual('Overall Performance');
    });
  });
});
