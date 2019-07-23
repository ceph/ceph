import { Helper } from '../helper.po';

describe('Images page', () => {
  let images;

  beforeAll(() => {
    images = new Helper().images;
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb and tab tests', () => {
    beforeAll(() => {
      images.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(images.getBreadcrumbText()).toEqual('Images');
    });

    it('should show three tabs', () => {
      expect(images.getTabsCount()).toEqual(3);
    });

    it('should show text for all tabs', () => {
      expect(images.getTabText(0)).toEqual('Images');
      expect(images.getTabText(1)).toEqual('Trash');
      expect(images.getTabText(2)).toEqual('Overall Performance');
    });
  });
});
