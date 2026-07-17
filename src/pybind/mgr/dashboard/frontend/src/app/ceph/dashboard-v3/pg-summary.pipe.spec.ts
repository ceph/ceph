import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '~/testing/unit-test-helper';
import { PgCategoryService } from '../shared/pg-category.service';
import { PgSummaryPipe } from './pg-summary.pipe';

describe('PgSummaryPipe', () => {
  let pipe: PgSummaryPipe;

  configureTestBed({
    providers: [PgSummaryPipe, PgCategoryService]
  });

  beforeEach(() => {
    pipe = TestBed.inject(PgSummaryPipe);
  });

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('tranforms value', () => {
    const value = {
      statuses: [
        {
          state_name: 'active+clean',
          count: 497
        }
      ],
      total: 497
    };
    expect(pipe.transform(value)).toEqual({
      categoryPgAmount: {
        clean: 497
      },
      total: 497
    });
  });
});
