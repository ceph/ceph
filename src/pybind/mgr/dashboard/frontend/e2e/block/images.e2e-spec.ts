import { Helper } from '../helper.po';
import { ImagesPage } from './images.po';

describe('Images page', () => {
  let page: ImagesPage;

  beforeAll(() => {
    page = new ImagesPage();
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb and tab tests', () => {
    beforeAll(() => {
      page.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(Helper.getBreadcrumbText()).toEqual('Images');
    });

    it('should show three tabs', () => {
      expect(Helper.getTabsCount()).toEqual(3);
    });

    it('should show text for all tabs', () => {
      expect(Helper.getTabText(0)).toEqual('Images');
      expect(Helper.getTabText(1)).toEqual('Trash');
      expect(Helper.getTabText(2)).toEqual('Overall Performance');
    });
  });
});
