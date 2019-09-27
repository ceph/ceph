import { NfsPageHelper } from './nfs.po';

describe('Nfs page', () => {
  let nfs: NfsPageHelper;

  beforeAll(() => {
    nfs = new NfsPageHelper();
  });

  afterEach(async () => {
    await NfsPageHelper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(async () => {
      await nfs.navigateTo();
    });

    it('should open and show breadcrumb', async () => {
      await nfs.waitTextToBePresent(nfs.getBreadcrumb(), 'NFS');
    });
  });
});
