import { Helper } from '../helper.po';
import { PageHelper } from '../page-helper.po';

describe('RGW buckets page', () => {
  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(() => {
      Helper.buckets.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(PageHelper.getBreadcrumbText()).toEqual('Buckets');
    });
  });

  describe('create, edit & delete bucket test', () => {
    beforeAll(() => {
      Helper.buckets.navigateTo();
    });

    it('should create bucket', () => {
      Helper.buckets.create('000test', 'testid');
      expect(PageHelper.getTableCell('000test').isPresent()).toBe(true);
    });

    it('should edit bucket', () => {
      Helper.buckets.edit('000test', 'dev');
      expect(PageHelper.getTable().getText()).toMatch('dev');
    });

    it('should delete bucket', () => {
      Helper.buckets.delete('000test');
      expect(PageHelper.getTableCell('000test').isPresent()).toBe(false);
    });
  });

  describe('Invalid Input in Create and Edit tests', () => {
    beforeAll(() => {
      Helper.buckets.navigateTo();
    });

    it('should test invalid inputs in create fields', () => {
      Helper.buckets.invalidCreate();
    });
    it('should test invalid input in edit owner field', () => {
      Helper.buckets.create('000rq', 'dev');
      Helper.buckets.invalidEdit('000rq');
      Helper.buckets.delete('000rq');
    });
  });
});
