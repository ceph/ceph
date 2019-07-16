import { Helper } from '../helper.po';

describe('Monitors page', () => {
  let monitors;

  beforeAll(() => {
    monitors = new Helper().monitors;
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(() => {
      monitors.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(monitors.getBreadcrumbText()).toEqual('Monitors');
    });
  });
});
