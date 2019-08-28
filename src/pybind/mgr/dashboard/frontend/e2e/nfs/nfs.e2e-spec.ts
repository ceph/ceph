import { Helper } from '../helper.po';
import { NfsPageHelper } from './nfs.po';

describe('Nfs page', () => {
  let nfs: NfsPageHelper;

  beforeAll(() => {
    nfs = new NfsPageHelper();
  });

  afterEach(async () => {
    await Helper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(async () => {
      await nfs.navigateTo();
    });

    it('should open and show breadcrumb', async () => {
      await expect(nfs.getBreadcrumbText()).toEqual('NFS');
    });
  });
});
