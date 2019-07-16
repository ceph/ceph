import { Helper } from '../helper.po';

describe('CRUSH map page', () => {
  let crushmap;

  beforeAll(() => {
    crushmap = new Helper().crushmap;
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(() => {
      crushmap.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(crushmap.getBreadcrumbText()).toEqual('CRUSH map');
    });
  });
});
