import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { AuthGuardService } from './shared/services/auth-guard.service';
import { LoginComponent } from './core/auth/login/login.component';
import { HostsComponent } from './ceph/host/hosts/hosts.component';

const routes: Routes = [
  { path: '', redirectTo: 'hosts', pathMatch: 'full' },
  { path: 'login', component: LoginComponent },
  { path: 'hosts', component: HostsComponent, canActivate: [AuthGuardService] }
];

@NgModule({
  imports: [RouterModule.forRoot(routes, {useHash: true})],
  exports: [RouterModule]
})
export class AppRoutingModule { }
