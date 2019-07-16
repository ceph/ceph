import { Helper } from '../helper.po';

describe('Filesystems page', () => {
  let filesystems;

  beforeAll(() => {
    filesystems = new Helper().filesystems;
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(() => {
      filesystems.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(filesystems.getBreadcrumbText()).toEqual('Filesystems');
    });
  });
});
