import { Helper } from '../helper.po';

describe('Filesystems page', () => {
  let filesystems: Helper['filesystems'];

  beforeAll(() => {
    filesystems = new Helper().filesystems;
  });

  afterEach(async () => {
    await Helper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(async () => {
      await filesystems.navigateTo();
    });

    it('should open and show breadcrumb', async () => {
      expect(await filesystems.getBreadcrumbText()).toEqual('Filesystems');
    });
  });
});
