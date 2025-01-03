import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '~/testing/unit-test-helper';
import { PgCategoryService } from '../shared/pg-category.service';
import { PgSummaryPipe } from './pg-summary.pipe';

describe('OsdSummaryPipe', () => {
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
      statuses: {
        'active+clean': 241
      },
      pgs_per_osd: 241
    };
    expect(pipe.transform(value)).toEqual({
      categoryPgAmount: {
        clean: 241
      },
      total: 241
    });
  });
});
