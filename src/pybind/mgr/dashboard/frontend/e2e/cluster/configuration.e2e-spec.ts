import { Helper } from '../helper.po';

describe('Configuration page', () => {
  let configuration;

  beforeAll(() => {
    configuration = new Helper().configuration;
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(() => {
      configuration.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(configuration.getBreadcrumbText()).toEqual('Configuration');
    });
  });
});
