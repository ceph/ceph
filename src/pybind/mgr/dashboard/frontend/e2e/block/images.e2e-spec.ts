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
      expect(ImagesPage.getBreadcrumbText()).toEqual('Images');
    });

    it('should show three tabs', () => {
      expect(ImagesPage.getTabsCount()).toEqual(3);
    });

    it('should show text for all tabs', () => {
      expect(ImagesPage.getTabText(0)).toEqual('Images');
      expect(ImagesPage.getTabText(1)).toEqual('Trash');
      expect(ImagesPage.getTabText(2)).toEqual('Overall Performance');
    });
  });
});
