import { Helper } from '../helper.po';
import { MirroringPage } from './mirroring.po';

describe('Mirroring page', () => {
  let page: MirroringPage;

  beforeAll(() => {
    page = new MirroringPage();
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb and tab tests', () => {
    beforeAll(() => {
      page.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(MirroringPage.getBreadcrumbText()).toEqual('Mirroring');
    });

    it('should show three tabs', () => {
      expect(MirroringPage.getTabsCount()).toEqual(3);
    });

    it('should show text for all tabs', () => {
      expect(MirroringPage.getTabText(0)).toEqual('Issues');
      expect(MirroringPage.getTabText(1)).toEqual('Syncing');
      expect(MirroringPage.getTabText(2)).toEqual('Ready');
    });
  });
});
