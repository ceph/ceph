import { Helper } from '../helper.po';

describe('Mirroring page', () => {
  let mirroring;

  beforeAll(() => {
    mirroring = new Helper().mirroring;
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb and tab tests', () => {
    beforeAll(() => {
      mirroring.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(mirroring.getBreadcrumbText()).toEqual('Mirroring');
    });

    it('should show three tabs', () => {
      expect(mirroring.getTabsCount()).toEqual(3);
    });

    it('should show text for all tabs', () => {
      expect(mirroring.getTabText(0)).toEqual('Issues');
      expect(mirroring.getTabText(1)).toEqual('Syncing');
      expect(mirroring.getTabText(2)).toEqual('Ready');
    });
  });
});
