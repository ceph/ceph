import { Helper } from '../helper.po';
import { DaemonsPage } from './daemons.po';

describe('RGW daemons page', () => {
  let page: DaemonsPage;

  beforeAll(() => {
    page = new DaemonsPage();
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(() => {
      page.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(Helper.getBreadcrumbText()).toEqual('Daemons');
    });
  });
});
