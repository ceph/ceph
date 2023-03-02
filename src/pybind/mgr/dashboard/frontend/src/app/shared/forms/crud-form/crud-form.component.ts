import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { DataGatewayService } from '~/app/shared/services/data-gateway.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { Location } from '@angular/common';
import { FormGroup } from '@angular/forms';
import { mergeMap } from 'rxjs/operators';
import { CrudTaskInfo, JsonFormUISchema } from './crud-form.model';
import { Observable } from 'rxjs';
import _ from 'lodash';

@Component({
  selector: 'cd-crud-form',
  templateUrl: './crud-form.component.html',
  styleUrls: ['./crud-form.component.scss']
})
export class CrudFormComponent implements OnInit {
  model: any = {};
  resource: string;
  task: { message: string; id: string } = { message: '', id: '' };
  form = new FormGroup({});
  formUISchema$: Observable<JsonFormUISchema>;

  constructor(
    private dataGatewayService: DataGatewayService,
    private activatedRoute: ActivatedRoute,
    private taskWrapper: TaskWrapperService,
    private location: Location
  ) {}

  ngOnInit(): void {
    this.formUISchema$ = this.activatedRoute.data.pipe(
      mergeMap((data: any) => {
        this.resource = data.resource;
        return this.dataGatewayService.form(`ui-${this.resource}`);
      })
    );
  }

  submit(data: any, taskInfo: CrudTaskInfo) {
    if (data) {
      let taskMetadata = {};
      _.forEach(taskInfo.metadataFields, (field) => {
        taskMetadata[field] = data[field];
      });
      taskMetadata['__message'] = taskInfo.message;
      this.taskWrapper
        .wrapTaskAroundCall({
          task: new FinishedTask('crud-component', taskMetadata),
          call: this.dataGatewayService.create(this.resource, data)
        })
        .subscribe({
          complete: () => {
            this.location.back();
          }
        });
    }
  }
}
