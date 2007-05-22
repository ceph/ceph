#!/usr/bin/perl

my $epoch = shift || die "specify epoch";

my %auth;    # mds -> id -> replica -> nonce
my %replica; # mds -> id -> auth -> nonce

print "reading\n";
for (my $i=0; -e "cachedump.$epoch.mds$i"; $i++) {
	open(O,"cachedump.$epoch.mds$i");
	while (<O>) {
		my ($name,$s);
		($name,$s) = /^\[(inode \d+) \S+ (\S+)/;
		($name,$s) = /^\[(dir \d+) \S+ (\S+)/ unless $name;
		($name,$s) = /^\[dentry (\S+) (\S+)/ unless $name;
		if ($name) {
			if ($s =~ /^auth/) {
				$auth{$i}->{$name} = {};
				my ($rl) = $s =~ /\{(.*)\}/;
				for my $r (split(/,/,$rl)) {
					my ($who,$nonce) = $r =~ /(\d+)\=(\d+)/;
					$auth{$i}->{$name}->{$who} = $nonce;
					#print "auth $name rep by $who $nonce $s\n";
				}
			}
			else {
				my ($a,$b,$n) = $s =~ /rep@(\d+)\,([\-\d]+)\.(\d+)/;
				die $_ unless $a >= 0;
				$replica{$i}->{$name}->{$a} = $n;
				if ($b >= 0) {
					$replica{$i}->{$name}->{$b} = $n;
				}
			}
		}
	}
}

print "verifying replicas\n";
for my $mds (keys %replica) {
	for my $name (keys %{$replica{$mds}}) {
		for my $auth (keys %{$replica{$mds}->{$name}}) {
			if ($auth{$auth}->{$name}->{$mds}) {
				if ($auth{$auth}->{$name}->{$mds} < $replica{$mds}->{$name}->{$auth}) {
					print "problem: mds$mds has $name from mds$auth nonce $replica{$mds}->{$name}->{$auth}, auth has nonce $auth{$auth}->{$name}->{$mds}\n";
				} else {
					print "ok: mds$mds has $name from mds$auth nonce $replica{$mds}->{$name}->{$auth}, auth has nonce $auth{$auth}->{$name}->{$mds}\n";
				}
			} else {
				print "??: mds$mds has $name from mds$auth nonce $replica{$mds}->{$name}->{$auth}, auth has no nonce\n";
			}

		}
	}
}


