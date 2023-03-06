import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { DataGatewayService } from '~/app/shared/services/data-gateway.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { Location } from '@angular/common';
import { FormGroup } from '@angular/forms';
import { FormlyFormOptions } from '@ngx-formly/core';
import { mergeMap } from 'rxjs/operators';
import { JsonFormUISchema } from './crud-form.model';
import { Observable } from 'rxjs';

@Component({
  selector: 'cd-crud-form',
  templateUrl: './crud-form.component.html',
  styleUrls: ['./crud-form.component.scss']
})
export class CrudFormComponent implements OnInit {
  model: any = {};
  options: FormlyFormOptions = {};
  resource: string;
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

  submit(data: any) {
    if (data) {
      this.taskWrapper
        .wrapTaskAroundCall({
          task: new FinishedTask('ceph-user/create', {
            user_entity: data.user_entity
          }),
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
