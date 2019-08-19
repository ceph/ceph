import { Helper } from '../helper.po';

describe('Nfs page', () => {
  let nfs: Helper['nfs'];

  beforeAll(() => {
    nfs = new Helper().nfs;
  });

  afterEach(async () => {
    await Helper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(async () => {
      await nfs.navigateTo();
    });

    it('should open and show breadcrumb', async () => {
      expect(await nfs.getBreadcrumbText()).toEqual('NFS');
    });
  });
});
