import { Component, OnInit, AfterViewInit } from '@angular/core';
import { Router } from '@angular/router';
import { FormControl, Validators } from '@angular/forms';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { NfsService } from '~/app/shared/api/nfs.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { HostService } from '~/app/shared/api/host.service';
import { forkJoin, Observable } from 'rxjs';
import { map } from 'rxjs/operators';
// URLBuilderService is not provided for routed components; use Router with absolute path instead

@Component({
  selector: 'cd-nfs-cluster-form',
  templateUrl: './nfs-cluster-form.component.html',
  styleUrls: ['./nfs-cluster-form.component.scss']
})
export class NfsClusterFormComponent implements OnInit, AfterViewInit {
  clusterId = new FormControl('', [Validators.required]);
  placement = new FormControl('');
  // default count to 1
  count = new FormControl(1, [Validators.min(1)]);
  ingress = new FormControl(false);
  virtual_ip = new FormControl('');
  ingress_mode = new FormControl('default');
  port = new FormControl('', [Validators.min(1)]);

  ingressModes = [
    { value: 'default', label: 'default' },
    { value: 'keepalive-only', label: 'keepalive-only' },
    { value: 'haproxy-standard', label: 'haproxy-standard' },
    { value: 'haproxy-protocol', label: 'haproxy-protocol' }
  ];


  hostsAndLabels$: Observable<{ hosts: any[]; labels: any[] }>;
  selectedHosts: string[] = [];
  selectedLabels: string[] = [];
  // surface API/form errors to the template
  formError: string | null = null;

  constructor(
    private nfsService: NfsService,
    private taskWrapper: TaskWrapperService,
    private router: Router,
    private hostService: HostService,
    private notificationService: NotificationService,
    private orchService: OrchestratorService
  ) {}

  ngOnInit(): void {}

  ngAfterViewInit() {
    // load hosts and labels for placement suggestions
    this.hostsAndLabels$ = forkJoin({
      hosts: this.hostService.getAllHosts(),
      labels: this.hostService.getLabels()
    }).pipe(
      map(({ hosts, labels }) => ({
        hosts: hosts.map((host: any) => ({ content: host['hostname'] })),
        labels: labels.map((label: string) => ({ content: label }))
      }))
    );

    // set default placement if orchestrator is available (similar UX to SMB)
    this.orchService.status().subscribe((status) => {
      if (status?.available) {
        // default to hosts placement when orchestrator is present
        this.placement.setValue('hosts');
      } else {
        // prefer a concrete default (hosts) rather than an empty/none option
        this.placement.setValue('hosts');
      }
    });

    // make virtual_ip required when ingress is enabled
    this.ingress.valueChanges.subscribe((enabled) => {
      if (enabled) {
        this.virtual_ip.setValidators([Validators.required]);
      } else {
        this.virtual_ip.clearValidators();
      }
      this.virtual_ip.updateValueAndValidity();
    });
  }


  validateIngressVirtualIp(): boolean {
    if (this.ingress.value && !this.virtual_ip.value) {
      return false;
    }
    return true;
  }

  onHostsInput(value: string) {
    this.selectedHosts = (value || '').split(',').map((s) => s.trim()).filter(Boolean);
  }

  onLabelsInput(value: string) {
    this.selectedLabels = (value || '').split(',').map((s) => s.trim()).filter(Boolean);
  }

  multiSelector(event: any, field: 'hosts' | 'label') {
    if (field === 'hosts') this.selectedHosts = event.map((h: any) => h.content);
    else this.selectedLabels = event.map((l: any) => l.content);
  }

  submit() {
    // mark validation and prevent submit if required fields invalid
    this.clusterId.markAsTouched();
    this.count.markAsTouched();
    this.port.markAsTouched();
    this.virtual_ip.markAsTouched();
    if (this.clusterId.invalid || this.count.invalid || this.port.invalid || this.virtual_ip.invalid) {
      return;
    }

    // build placement payload: prefer explicit placement selection (hosts/label) using selected items
    // The API expects placement as an object, e.g. { hosts: ["ceph-node-00"], count: 1 }
    let placementPayload: any | undefined = undefined;
    const placementVal = this.placement.value;
    const countVal = this.count?.value;
    if (placementVal === 'hosts' && this.selectedHosts?.length > 0) {
      // prefer object form with hosts array and count (default 1)
      placementPayload = { hosts: this.selectedHosts.slice() };
      placementPayload.count = countVal && Number(countVal) > 0 ? Number(countVal) : 1;
    } else if (placementVal === 'label' && this.selectedLabels?.length > 0) {
      // use label + count form
      placementPayload = { label: this.selectedLabels[0] };
      placementPayload.count = countVal && Number(countVal) > 0 ? Number(countVal) : 1;
    } else if (this.placement.value && ['hosts', 'label'].indexOf(this.placement.value) === -1) {
      // allow raw placement string when provided (fallback), but do not
      // pass the reserved tokens 'hosts' or 'label' as raw placement values
      placementPayload = this.placement.value;
    }

    // If count is 1, don't pass placement to the API (default behavior)
    if (placementPayload && typeof placementPayload === 'object' && placementPayload.count === 1) {
      placementPayload = undefined;
    }

    const ingressBool = !!this.ingress.value;

    const payload: any = {
      cluster_id: this.clusterId.value,
      ingress: ingressBool,
      port: this.port.value || undefined
    };

    if (placementPayload !== undefined) {
      payload.placement = placementPayload;
    }

    if (ingressBool) {
      if (this.virtual_ip.value) payload.virtual_ip = this.virtual_ip.value;
      if (this.ingress_mode.value) payload.ingress_mode = this.ingress_mode.value;
    }

  // Use the TaskMessageService key for NFS create so notification titles are correct
  const task = new FinishedTask('nfs/create', { cluster_id: payload.cluster_id });
    this.formError = null;
    const wrapped$ = this.taskWrapper.wrapTaskAroundCall({ task, call: this.nfsService.createCluster(payload) });

    wrapped$.subscribe({
      next: () => {
        // wrapTaskAroundCall does not emit next by design in many cases,
        // but if it does, navigate immediately
        // eslint-disable-next-line no-console
        console.debug('nfs cluster create: next');
        try {
          const cfg = this.notificationService.finishedTaskToNotification(task as any, true);
          // ensure this finished notification is also persisted in the notification drawer
          cfg.isFinishedTask = false;
          this.notificationService.show(cfg);
        } catch (e) {
          // ignore notification build errors
        }
        setTimeout(() => this.router.navigate(['/cephfs/nfs/cluster']), 0);
      },
      complete: () => {
        // the wrapper signals completion for success paths
        // eslint-disable-next-line no-console
        console.debug('nfs cluster create: complete');
        try {
          const cfg = this.notificationService.finishedTaskToNotification(task as any, true);
          cfg.isFinishedTask = false;
          this.notificationService.show(cfg);
        } catch (e) {
          // ignore
        }
        setTimeout(() => this.router.navigate(['/cephfs/nfs/cluster']), 0);
      },
      error: (err) => {
        // surface the error to the user and log for debugging
        // eslint-disable-next-line no-console
        console.error('nfs cluster create failed', err);
        // attempt to extract a readable message
        if (err && err.error && typeof err.error === 'string') {
          this.formError = err.error;
        } else if (err && err.message) {
          this.formError = err.message;
        } else {
          this.formError = 'Failed to create NFS cluster';
        }
      }
    });
  }
}
