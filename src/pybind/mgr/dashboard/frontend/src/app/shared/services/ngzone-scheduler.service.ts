import { Injectable, NgZone } from '@angular/core';

import { asyncScheduler, SchedulerLike, Subscription } from 'rxjs';

abstract class NgZoneScheduler implements SchedulerLike {
  protected scheduler = asyncScheduler;

  constructor(protected zone: NgZone) {}

  abstract schedule(...args: any[]): Subscription;

  now(): number {
    return this.scheduler.now();
  }
}

@Injectable({
  providedIn: 'root'
})
export class LeaveNgZoneScheduler extends NgZoneScheduler {
  constructor(zone: NgZone) {
    super(zone);
  }

  schedule(...args: any[]): Subscription {
    return this.zone.runOutsideAngular(() => this.scheduler.schedule.apply(this.scheduler, args));
  }
}

@Injectable({
  providedIn: 'root'
})
export class EnterNgZoneScheduler extends NgZoneScheduler {
  constructor(zone: NgZone) {
    super(zone);
  }

  schedule(...args: any[]): Subscription {
    return this.zone.run(() => this.scheduler.schedule.apply(this.scheduler, args));
  }
}

@Injectable({
  providedIn: 'root'
})
export class NgZoneSchedulerService {
  constructor(public leave: LeaveNgZoneScheduler, public enter: EnterNgZoneScheduler) {}
}
