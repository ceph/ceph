import { Helper } from '../helper.po';
import { ConfigurationPage } from './configuration.po';

describe('Configuration page', () => {
  let page: ConfigurationPage;

  beforeAll(() => {
    page = new ConfigurationPage();
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(() => {
      page.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      Helper.waitTextToBePresent(Helper.getBreadcrumb(), 'Configuration');
    });
  });
});
