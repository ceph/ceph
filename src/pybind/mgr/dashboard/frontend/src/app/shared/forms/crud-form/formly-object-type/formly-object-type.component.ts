/**  Copyright 2021 Formly. All Rights Reserved.
    Use of this source code is governed by an MIT-style license that
    can be found in the LICENSE file at https://github.com/ngx-formly/ngx-formly/blob/main/LICENSE */

import { Component } from '@angular/core';
import { FieldType } from '@ngx-formly/core';

@Component({
  selector: 'cd-formly-object-type',
  templateUrl: './formly-object-type.component.html',
  styleUrls: ['./formly-object-type.component.scss']
})
export class FormlyObjectTypeComponent extends FieldType {
  get inputClass(): string {
    const layoutType = this.props.templateOptions?.layoutType;
    const defaultFlexClasses = 'd-flex justify-content-center align-content-stretch gap-3';
    if (layoutType == 'row') {
      return defaultFlexClasses + ' flex-row';
    }
    return defaultFlexClasses + ' flex-column';
  }
}
