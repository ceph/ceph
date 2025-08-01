import { Component } from '@angular/core';
import { HttpClient } from '@angular/common/http';

// import {
//   ButtonModule,
//   CheckboxModule,
//   GridModule,
//   IconModule,
//   IconService,
//   InputModule,
//   ModalModule,
//   SelectModule
// } from 'carbon-components-angular';

// import ArrowRight16 from '@carbon/icons/es/arrow--right/16';


@Component({
  selector: 'cd-cli-terminal',
  templateUrl: './cli-terminal.component.html',
  styleUrls: ['./cli-terminal.component.scss'],
})
export class CliTerminalComponent {
  
  command = '';
  show = false;
  history: { cmd: string; stdout: string; stderr: string; code: number }[] = [];
  watch = false;
  watchInterval: any = null;
  watchTimer: number = 5000; // 5 seconds
  filteredCommands: string[] = [];
  availableCommands: string[] = [
    // General Commands
    'ceph status', 'ceph health', 'ceph osd status', 'ceph osd tree', 'ceph osd df', 
    'ceph osd lspools', 'ceph osd pool create <pool_name> <pg_num>', 'ceph osd pool delete <pool_name>', 
    'ceph auth list', 'ceph auth add', 'ceph mon stat', 'ceph daemon <daemon_id> status', 
    'ceph pg stat', 'ceph upgrade check',

    // OSD Commands
    'ceph osd crush add <osd_id> <weight>', 'ceph osd reweight <osd_id> <weight>', 'ceph osd repair <osd_id>', 
    'ceph osd blacklist', 'ceph osd down',

    // Monitor Commands
    'ceph mon stat', 'ceph mon add', 'ceph mon remove',

    // MDS Commands
    'ceph mds stat', 'ceph mds fail', 'ceph mds reset',

    // Adding radosgw-admin commands
    'radosgw-admin user create --uid=<user_id> --display-name=<display_name>',
    'radosgw-admin user modify --uid=<user_id> --display-name=<display_name>',
    'radosgw-admin user rm --uid=<user_id>',
    'radosgw-admin user stats --uid=<user_id>',
    'radosgw-admin user quota --uid=<user_id>',
    'radosgw-admin bucket stats --bucket=<bucket_name>',
    'radosgw-admin bucket create --bucket=<bucket_name>',
    'radosgw-admin bucket rm --bucket=<bucket_name>',
    'radosgw-admin object stat --bucket=<bucket_name> --object=<object_name>',
    'radosgw-admin object delete --bucket=<bucket_name> --object=<object_name>',
    'radosgw-admin zonegroup stats',
    'radosgw-admin zonegroup create --name=<zone_group_name>',
    'radosgw-admin zonegroup rm --name=<zone_group_name>',
    'radosgw-admin zone stats',
    'radosgw-admin zone create --name=<zone_name>',
    'radosgw-admin zone rm --name=<zone_name>',
    'radosgw-admin s3 bucket stats --bucket=<bucket_name>',
    'radosgw-admin s3 sync --src-bucket=<source_bucket> --dst-bucket=<destination_bucket>',
    'radosgw-admin log level --level=<level>',
    'radosgw-admin log tail',
    'radosgw-admin sync status',
    'radosgw-admin sync start --zone=<zone_name>',
    'radosgw-admin sync stop --zone=<zone_name>',

    // CephFS Commands
    'ceph fs status', 'ceph fs volume create <volume_name>', 'ceph fs volume delete <volume_name>',
    'ceph fs subvolume create <volume_name> <subvolume_name>', 'ceph fs subvolume rm <volume_name> <subvolume_name>',
    'ceph fs subvolumegroup create <volume_name> <group_name>', 'ceph fs subvolumegroup rm <volume_name> <group_name>',
    'ceph fs snapshot create <volume_name> <snapshot_name>', 'ceph fs snapshot rm <volume_name> <snapshot_name>',
    'ceph fs snapshot ls <volume_name>', 'ceph fs mirror enable <volume_name>', 
    'ceph fs mirror disable <volume_name>', 'ceph fs mirror status <volume_name>',

    // RBD Commands
    'rbd create <image_name> --size <size_in_mb>', 'rbd rm <image_name>', 
    'rbd snap create <image_name>@<snapshot_name>', 'rbd snap rollback <image_name>@<snapshot_name>',
    'rbd info <image_name>', 'rbd ls', 'rbd map <image_name>', 'rbd unmap <image_name>',
    'rbd resize <image_name> --size <new_size_in_mb>', 'rbd snap ls <image_name>',
    'rbd snap remove <image_name>@<snapshot_name>', 'rbd snap protect <image_name>@<snapshot_name>',
    'rbd snap unprotect <image_name>@<snapshot_name>', 'rbd clone <parent_image>@<snapshot_name> <new_image>',
    'rbd flatten <image_name>', 'rbd bench <image_name> --io-type <read|write> --io-size <size_in_kb>',
    'rbd mirror image enable <image_name>', 'rbd mirror image disable <image_name>',
    'rbd mirror pool enable <pool_name>', 'rbd mirror pool disable <pool_name>',
    'rbd mirror image status <image_name>', 'rbd mirror pool status <pool_name>',
    'rbd perf image iostat <image_name>', 'rbd perf pool iostat <pool_name>',

    // Dashboard Commands
    'ceph dashboard', 'ceph dashboard set-login <user> <password>', 'ceph dashboard status', 
    'ceph dashboard enable', 'ceph dashboard disable', 'ceph dashboard set-url-prefix <prefix>', 
    'ceph dashboard get-url-prefix', 'ceph dashboard set-ssl-certificate <certificate_path>', 
    'ceph dashboard get-ssl-certificate', 'ceph dashboard ac-user-create <username> <password> <role>', 
    'ceph dashboard ac-user-delete <username>', 'ceph dashboard ac-user-set-password <username> <password>', 
    'ceph dashboard ac-user-set-roles <username> <role1,role2,...>', 'ceph dashboard ac-user-show <username>', 
    'ceph dashboard ac-user-list', 'ceph dashboard ac-role-create <role_name> <permissions>', 
    'ceph dashboard ac-role-delete <role_name>', 'ceph dashboard ac-role-set-permissions <role_name> <permissions>', 
    'ceph dashboard ac-role-show <role_name>', 'ceph dashboard ac-role-list',
    
    // Ceph Orchestrator Commands
    'ceph orch status', 'ceph orch apply -i <filename>', 'ceph orch upgrade', 'ceph orch ls',
    'ceph orch ps', 'ceph orch daemon add <service> <daemon_type>', 'ceph orch daemon rm <daemon_id>',
    'ceph orch service rm <service_name>', 'ceph orch service add <service_name>',
    'ceph orch host ls', 'ceph orch host add <hostname>', 'ceph orch host rm <hostname>',
    'ceph orch device ls', 'ceph orch daemon add osd <host>', 'ceph orch daemon add mon <host>',
    'ceph orch daemon rm osd <osd_id>', 'ceph orch daemon rm mon <mon_id>',
    'ceph orch service status', 'ceph orch service ls', 'ceph orch service add mon', 
    'ceph orch service add osd', 'ceph orch service rm <service>',
    'ceph orch host add <hostname>', 'ceph orch host rm <hostname>', 'ceph orch device claim <device>',
    
    // NFS Commands (Ceph NFS)
    'ceph nfs status', 'ceph nfs create', 'ceph nfs disable', 'ceph nfs addpool <pool_name>', 
    'ceph nfs removepool <pool_name>', 'ceph nfs show', 'ceph nfs client <client_id> grant', 
    'ceph nfs client <client_id> revoke', 'ceph nfs export create <export_name> <path>', 
    'ceph nfs export delete <export_name>', 'ceph nfs export list', 'ceph nfs export modify <export_name> <options>',
    'ceph nfs export show <export_name>', 'ceph nfs export set <export_name> <key> <value>',

    // QOS Commands
    'ceph qos status', 'ceph qos set <pool_name> <key> <value>', 'ceph qos get <pool_name>', 
    'ceph qos remove <pool_name> <key>', 'ceph qos list', 'ceph qos apply <pool_name>',
    'ceph qos reset <pool_name>',

    // SMB Commands (Ceph SMB)
    'ceph smb status', 'ceph smb enable', 'ceph smb disable', 'ceph smb user create <user_id>',
    'ceph smb user modify <user_id>', 'ceph smb user delete <user_id>', 'ceph smb domain add <domain>',
    'ceph smb domain delete <domain>', 'ceph smb share add <share_name>', 'ceph smb share remove <share_name>',
    
    // NVMe Commands (Ceph NVMe)
    'ceph nvme status', 'ceph nvme device add <device_name>', 'ceph nvme device remove <device_name>',
    'ceph nvme device list', 'ceph nvme device claim <device_name>', 'ceph nvme device unclaim <device_name>',
    'ceph nvme device replace <device_name>',
    'ceph osd crush add nvme <device_id> <weight>',
  ];

  constructor(private http: HttpClient) {}
  
  runCommand() {
    const cmd = this.command;
    this.command = ''; // Clear input
    if(cmd !== '') {
      this.http.post<any>('/api/cli/execute', { command: cmd})
      .subscribe(res => {
        this.history.push({
          cmd,
          stdout: res.stdout,
          stderr: res.stderr,
          code: res.code
        });
        if (this.watch && !this.watchInterval) {
          this.watchInterval = setInterval(() => {
            this.http.post<any>('/api/cli/execute', { command: cmd}).subscribe({
              next: (res) => {
                this.history.push({
                  cmd,
                  stdout: res.stdout,
                  stderr: res.stderr,
                  code: res.code
                });
              },
              error: () => {
                this.history.push({
                  cmd,
                  stdout: res.stdout,
                  stderr: 'Error running command.',
                  code: res.code
                });
              }
            });
          }, this.watchTimer);
        }
      });

    }
  }

  onInputChange() {
    this.filteredCommands = this.availableCommands.filter(command =>
      command.toLowerCase().includes(this.command.toLowerCase())
    );
  }

  selectCommand(command: string) {
    this.command = command;
    this.runCommand()
    this.filteredCommands = [];
    this.command = '';
  }

  toggleWatch() {
    if (!this.watch && this.watchInterval) {
      clearInterval(this.watchInterval);
      this.watchInterval = null;
    }
  }

  clearHistory() {
    this.history = [];
  }

  ngOnDestroy() {
    // Clear watch interval on component destroy
    if (this.watchInterval) {
      clearInterval(this.watchInterval);
    }
  }

  close() {
    this.show = false;
  }
  
  toggleCLI() {
    this.show = !this.show;
  }

}


