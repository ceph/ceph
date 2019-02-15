import { Injectable } from '@angular/core';

import * as _ from 'lodash';

import { CephSharedModule } from './ceph-shared.module';
import { PgCategory } from './pg-category.model';

@Injectable({
  providedIn: CephSharedModule
})
export class PgCategoryService {
  private categories: object;

  constructor() {
    this.categories = this.createCategories();
  }

  getAllTypes() {
    return PgCategory.VALID_CATEGORIES;
  }

  getTypeByStates(pgStatesText: string): string {
    const pgStates = this.getPgStatesFromText(pgStatesText);

    if (pgStates.length === 0) {
      return PgCategory.CATEGORY_UNKNOWN;
    }

    const intersections = _.zipObject(
      PgCategory.VALID_CATEGORIES,
      PgCategory.VALID_CATEGORIES.map(
        (category) => _.intersection(this.categories[category].states, pgStates).length
      )
    );

    if (intersections[PgCategory.CATEGORY_WARNING] > 0) {
      return PgCategory.CATEGORY_WARNING;
    }

    const pgWorkingStates = intersections[PgCategory.CATEGORY_WORKING];
    if (pgStates.length > intersections[PgCategory.CATEGORY_CLEAN] + pgWorkingStates) {
      return PgCategory.CATEGORY_UNKNOWN;
    }

    return pgWorkingStates ? PgCategory.CATEGORY_WORKING : PgCategory.CATEGORY_CLEAN;
  }

  private createCategories() {
    return _.zipObject(
      PgCategory.VALID_CATEGORIES,
      PgCategory.VALID_CATEGORIES.map((category) => new PgCategory(category))
    );
  }

  private getPgStatesFromText(pgStatesText) {
    const pgStates = pgStatesText
      .replace(/[^a-z]+/g, ' ')
      .trim()
      .split(' ');

    return _.uniq(pgStates);
  }
}
