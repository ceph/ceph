import { Helper } from '../helper.po';
import { PageHelper } from '../page-helper.po';

describe('RGW buckets page', () => {
  let helper: Helper;

  beforeAll(() => {
    helper = new Helper();
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(() => {
      helper.buckets.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(PageHelper.getBreadcrumbText()).toEqual('Buckets');
    });
  });

  describe('create, edit & delete bucket test', () => {
    beforeAll(() => {
      helper.buckets.navigateTo();
    });

    it('should create bucket', () => {
      helper.buckets.create('000test', 'testid');
      expect(PageHelper.getTableCell('000test').isPresent()).toBe(true);
    });

    it('should edit bucket', () => {
      helper.buckets.edit('000test', 'dev');
      expect(PageHelper.getTable().getText()).toMatch('dev');
    });

    it('should delete bucket', () => {
      helper.buckets.delete('000test');
      expect(PageHelper.getTableCell('000test').isPresent()).toBe(false);
    });
  });

  describe('Invalid Input in Create and Edit tests', () => {
    beforeAll(() => {
      helper.buckets.navigateTo();
    });

    it('should test invalid inputs in create fields', () => {
      helper.buckets.invalidCreate();
    });

    it('should test invalid input in edit owner field', () => {
      helper.buckets.create('000rq', 'dev');
      helper.buckets.invalidEdit('000rq');
      helper.buckets.delete('000rq');
    });
  });
});
