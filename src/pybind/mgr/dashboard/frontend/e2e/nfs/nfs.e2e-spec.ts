import { Helper } from '../helper.po';

describe('Nfs page', () => {
  let nfs;

  beforeAll(() => {
    nfs = new Helper().nfs;
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(() => {
      nfs.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(nfs.getBreadcrumbText()).toEqual('NFS');
    });
  });
});
