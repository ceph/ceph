import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { AuthGuardService } from './shared/services/auth-guard.service';
import { EmptyComponent } from './shared/empty/empty.component';

const routes: Routes = [
  // TODO configure an appropriate default route (maybe on ceph module?)
  { path: '', canActivate: [AuthGuardService], component: EmptyComponent },
];

@NgModule({
  imports: [RouterModule.forRoot(routes, {useHash: true})],
  exports: [RouterModule]
})
export class AppRoutingModule { }
