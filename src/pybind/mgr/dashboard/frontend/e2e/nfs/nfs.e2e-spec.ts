import { Helper } from '../helper.po';
import { NfsPage } from './nfs.po';

describe('Nfs page', () => {
  let page: NfsPage;

  beforeAll(() => {
    page = new NfsPage();
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(() => {
      page.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(Helper.getBreadcrumbText()).toEqual('NFS');
    });
  });
});
