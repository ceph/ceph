import { Helper } from '../helper.po';
import { OSDsPage } from './osds.po';

describe('OSDs page', () => {
  let page: OSDsPage;

  beforeAll(() => {
    page = new OSDsPage();
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb and tab tests', () => {
    beforeAll(() => {
      page.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(OSDsPage.getBreadcrumbText()).toEqual('OSDs');
    });

    it('should show two tabs', () => {
      expect(OSDsPage.getTabsCount()).toEqual(2);
    });

    it('should show OSDs list tab at first', () => {
      expect(OSDsPage.getTabText(0)).toEqual('OSDs List');
    });

    it('should show overall performance as a second tab', () => {
      expect(OSDsPage.getTabText(1)).toEqual('Overall Performance');
    });
  });
});
