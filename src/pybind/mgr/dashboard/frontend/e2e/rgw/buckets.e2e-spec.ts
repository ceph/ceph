import { Helper } from '../helper.po';
import { PageHelper } from '../page-helper.po';

describe('RGW buckets page', () => {
  let buckets;

  beforeAll(() => {
    buckets = new Helper().buckets;
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(() => {
      buckets.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(PageHelper.getBreadcrumbText()).toEqual('Buckets');
    });
  });

  describe('create, edit & delete bucket test', () => {
    beforeAll(() => {
      buckets.navigateTo();
    });

    it('should create bucket', () => {
      buckets.create('000test', '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef');
      expect(PageHelper.getTableCell('000test').isPresent()).toBe(true);
    });

    it('should edit bucket', () => {
      buckets.edit('000test', 'dev');
      expect(PageHelper.getTable().getText()).toMatch('dev');
    });

    it('should delete bucket', () => {
      buckets.delete('000test');
      expect(PageHelper.getTableCell('000test').isPresent()).toBe(false);
    });
  });

  describe('Invalid Input in Create and Edit tests', () => {
    beforeAll(() => {
      buckets.navigateTo();
    });

    it('should test invalid inputs in create fields', () => {
      buckets.invalidCreate();
    });

    it('should test invalid input in edit owner field', () => {
      buckets.create('000rq', '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef');
      buckets.invalidEdit('000rq');
      buckets.delete('000rq');
    });
  });
});
