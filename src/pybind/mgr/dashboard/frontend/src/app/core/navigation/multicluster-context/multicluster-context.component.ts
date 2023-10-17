import { Component, EventEmitter, Output } from '@angular/core';
import { MulticlusterService } from '~/app/shared/api/multicluster.service';
import _ from 'lodash';

@Component({
  selector: 'cd-multicluster-context',
  templateUrl: './multicluster-context.component.html',
  styleUrls: ['./multicluster-context.component.scss']
})
export class MulticlusterContextComponent {
  @Output() selectionChanged = new EventEmitter<string>();

  constructor(
    public multiClusterService: MulticlusterService,
  ) {
    
  }

  ngOnInit() {
    this.multiClusterService.getRemoteClusterUrls().subscribe((data) => {
      console.log(data);
      
    })
  }

  onClusterSelection(value: object) {    
    if (!_.isEmpty(value)) {
      this.multiClusterService.setConfigs(value).subscribe((data: any) => {
        window.location.reload();
        console.log(data);
      });
    }
    this.multiClusterService.emitSelectionChanged(value);
  }


  }
