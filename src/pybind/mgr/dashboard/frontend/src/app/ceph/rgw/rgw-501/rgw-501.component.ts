import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

@Component({
  selector: 'cd-rgw-501',
  templateUrl: './rgw-501.component.html',
  styleUrls: ['./rgw-501.component.scss']
})
export class Rgw501Component implements OnInit, OnDestroy {
  message = 'The Object Gateway service is not configured.';
  routeParamsSubscribe: any;

  constructor(private route: ActivatedRoute) {}

  ngOnInit() {
    this.routeParamsSubscribe = this.route.params.subscribe((params: { message: string }) => {
      this.message = params.message;
    });
  }

  ngOnDestroy() {
    this.routeParamsSubscribe.unsubscribe();
  }
}
