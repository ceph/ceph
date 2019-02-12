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
      expect(Helper.getBreadcrumbText()).toEqual('Mirroring');
    });

    it('should show three tabs', () => {
      expect(Helper.getTabsCount()).toEqual(3);
    });

    it('should show text for all tabs', () => {
      expect(Helper.getTabText(0)).toEqual('Issues');
      expect(Helper.getTabText(1)).toEqual('Syncing');
      expect(Helper.getTabText(2)).toEqual('Ready');
    });
  });
});
