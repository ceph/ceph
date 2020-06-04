import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { PgCategory } from './pg-category.model';
import { PgCategoryService } from './pg-category.service';

describe('PgCategoryService', () => {
  let service: PgCategoryService;

  configureTestBed({
    providers: [PgCategoryService]
  });

  beforeEach(() => {
    service = TestBed.inject(PgCategoryService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('returns all category types', () => {
    const categoryTypes = service.getAllTypes();

    expect(categoryTypes).toEqual(PgCategory.VALID_CATEGORIES);
  });

  describe('getTypeByStates', () => {
    const testMethod = (value: string, expected: string) =>
      expect(service.getTypeByStates(value)).toEqual(expected);

    it(PgCategory.CATEGORY_CLEAN, () => {
      testMethod('clean', PgCategory.CATEGORY_CLEAN);
    });

    it(PgCategory.CATEGORY_WORKING, () => {
      testMethod('clean+scrubbing', PgCategory.CATEGORY_WORKING);
      testMethod(
        '  8 active+clean+scrubbing+deep, 255 active+clean  ',
        PgCategory.CATEGORY_WORKING
      );
    });

    it(PgCategory.CATEGORY_WARNING, () => {
      testMethod('clean+scrubbing+down', PgCategory.CATEGORY_WARNING);
      testMethod('clean+scrubbing+down+nonMappedState', PgCategory.CATEGORY_WARNING);
    });

    it(PgCategory.CATEGORY_UNKNOWN, () => {
      testMethod('clean+scrubbing+nonMappedState', PgCategory.CATEGORY_UNKNOWN);
      testMethod('nonMappedState', PgCategory.CATEGORY_UNKNOWN);
      testMethod('', PgCategory.CATEGORY_UNKNOWN);
    });
  });
});
