/**  Copyright 2021 Formly. All Rights Reserved.
    Use of this source code is governed by an MIT-style license that
    can be found in the LICENSE file at https://github.com/ngx-formly/ngx-formly/blob/main/LICENSE */

import { Component, OnInit } from '@angular/core';
import { FieldArrayType } from '@ngx-formly/core';
import { forEach } from 'lodash';
import { IconSize } from '~/app/shared/enum/icons.enum';

@Component({
  selector: 'cd-formly-array-type',
  templateUrl: './formly-array-type.component.html',
  styleUrls: ['./formly-array-type.component.scss'],
  standalone: false
})
export class FormlyArrayTypeComponent extends FieldArrayType implements OnInit {
  iconSize = IconSize;

  ngOnInit(): void {
    this.propagateTemplateOptions();
  }

  addWrapper() {
    this.add();
    this.propagateTemplateOptions();
  }

  propagateTemplateOptions() {
    forEach(this.field.fieldGroup, (field) => {
      if (field.type == 'object') {
        field.props.templateOptions = this.props.templateOptions.objectTemplateOptions;
      }
    });
  }
}
