#!/usr/bin/env perl
#
# Author: Sergey Kovalyov (sergey.kovalyov@gmail.com)
#
use common::sense;
use Getopt::Long qw/:config pass_through/;
use POSIX;
use DBI;
use LWP::UserAgent::Cached;
use Readonly;
use IO::Handle;
use Encode;

my %opts;
GetOptions(
	'debug'     => \$opts{debug},
	'use-cache' => \$opts{use_cache},
);
die "unknown option(s): ", join ', ', @ARGV if @ARGV;

if ($opts{use_cache}) {
	$opts{cache_dir} = 'cache';
	mkdir $opts{cache_dir} unless -d $opts{cache_dir};
}

my $dbh = DBI->connect("dbi:mysql:de") or die "Cannot connect: $DBI::errstr";
$dbh->{PrintError} = 0;
$dbh->{RaiseError} = 1;

Readonly my $ua_str => 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:36.0) Gecko/20100101 Firefox/36.0';
my $ua = new LWP::UserAgent::Cached(
	agent => $ua_str,
	cache_dir => $opts{cache_dir},
	cachename_spec => {
		_body => undef,
		_headers => [],
	},
	on_uncached => sub { sleep 3 },
	recache_if => sub {
		my (undef, $name) = @_;
		say "#\tcache file: $name";
=comment
		my $content = do { open my $fh, '<', $name; local $/; <$fh> };
		if ($content =~ /^500 / and $content =~ /Client-Warning: Internal response/) {
			unlink $name;
			return 1;
		}
=cut
		return;
	},
);
$ua->timeout(30);
if ($opts{debug}) {
	autoflush STDOUT;
	$ua->show_progress(1);
}
Readonly %opts => %opts;



sub dump_data {
	my ($data, $name) = @_;

	say "#\n# $name dump:" if $name;
	foreach (sort keys %$data) {
		say "# $_ = ", $$data{$_} if defined $$data{$_};
	}
	say "#";
}



sub get_url {
	my ($url) = @_;

	$url = 'http://' . $url unless $url =~ /^http/i;
	say "# getting $url";
	my $resp;
	eval {
		local $SIG{ALRM} = sub { die "timeout reached" };
		alarm 10;
		$resp = $ua->get($url);
		alarm 0;
	};
	if ($@) {
		say "# catched: ", $@;
		return;
	}
	my $ct = $resp->header('Content-Type');
	if ($ct =~ m{application/(octet-stream|pdf|xml|(rss|rdf|atom|opensearchdescription)\+xml)|text/(xml|css)}) {
		return;
	} elsif ($ct !~ m{text/(plain|html)}) {
		die "unexpected content-type: $ct";
	}
	return $resp->decoded_content(ref => 1) if $resp->is_success;
}



sub process_page {
	my ($content_ref, $data) = @_;

	my $ok = qr/[^!\?'"\][}{)(\s:;,&<>\*\s\\]/;
	my (@emails) = $$content_ref =~ /($ok+\@$ok+)/;
	push @$data, @emails;
}



sub process_target {
	my ($target) = @_;

	my $data = [ ];
	say "# domain: ", $$target{domain};

	my $content_ref = get_url $$target{domain};
	return unless $$content_ref;
	process_page $content_ref, $data;

	my @urls =  grep { not /(gif|png|jpg|ico|css|exe|pdf|doc|xls)(\?|$)/i
		and not /^tel:/i } $$content_ref =~ / href=(?:"(.*?)"|'(.*?)')/igsm;

	my $limit = 50;
	my %seen = ($$target{domain} => 1);
	foreach my $url (@urls) {
		if ($url =~ /^mailto:/i) {
			$url =~ s%^mailto:(//)?%%;
			say "# mailto: $url";
			push @$data, $url;
			next;
		}
		$url = $$target{domain} . $url if $url =~ m{^/};
		$url =~ s/#.*//;
		next if $seen{$url};
		$seen{$url} = 1;
		unless ($url =~ /$$target{domain}/) {
			say "# skipping url: $url";
			next;
		}

		last if $limit < 1;
		say "# limit: $limit url: $url";
		--$limit;

		my $content_ref = get_url $url;
		return unless $$content_ref;
		process_page $content_ref, $data;
	}
	my %es;
	foreach (@$data) {
		s/&#(\d+);/chr $1/eg;
		s/^[-, .]+//g;
		s/[-, .]+$//g;
		$es{$_} = 1;
	}
	my $emails = join ', ', keys %es;
	$dbh->do("update advertisers set checked = 1, emails = ? where id = ?", undef, $emails, $$target{id});
}



# here we start
#
dump_data \%opts, 'options' if $opts{debug};
say "# ", strftime "%Y-%m-%d %H:%M:%S: started", localtime;

my $targets = $dbh->selectall_arrayref("select id, domain from advertisers where checked = 0 and sent = 0 limit 100", { Slice => {} });
foreach my $target (@$targets) {
	if ($$target{domain} =~ m{[^-:/.a-z0-9]}i) { # FIXME: skip cyrillic domains for now
		say "# skip: ", $$target{domain};
	} else {
		process_target $target;
	}
	say "#";
}

say "# exit";
exit;

