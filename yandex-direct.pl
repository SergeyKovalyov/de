#!/usr/bin/env perl
#
# Author: Sergey Kovalyov (sergey.kovalyov@gmail.com)
#
use common::sense;
use Getopt::Long qw/:config pass_through/;
use POSIX;
use DBI;
use URL::Encode qw/url_encode/;
use LWP::UserAgent::Cached;
use Readonly;
use IO::Handle;

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
		my $content = do { open my $fh, '<', $name; local $/; <$fh> };
		say "#\tcache file: $name";
		if ($content =~ /name="captcha_code"/) {
			unlink $name;
			return 1;
		}
	},
);
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



sub create_tables {
	say "# creating tables if they are not exist" if $opts{debug};
	$dbh->do("create table if not exists keywords (
			id int unsigned auto_increment primary key,
			keyword varchar(255) not null, unique(keyword)
		) engine = InnoDB");
	$dbh->do("create table if not exists locations (
			id int unsigned auto_increment primary key,
			ip_address varchar(15) not null, unique(ip_address)
		) engine = InnoDB");
	$dbh->do("create table if not exists advertisers (
			id int unsigned auto_increment primary key,
			domain varchar(255), unique(domain),
			name varchar(255),
			address varchar(255),
			city varchar(255),
			country varchar(255),
			phone varchar(255),
			coords varchar(255),
			title varchar(255),
			`text` varchar(255)
		) engine = InnoDB");
	$dbh->do("create table if not exists done (
			id int unsigned auto_increment primary key,
			keyword_id int unsigned,
			foreign key(keyword_id) references keywords(id),
			location_id int unsigned,
			foreign key(location_id) references locations(id),
			unique(keyword_id, location_id)
		) engine = InnoDB");
	$dbh->do("create table if not exists results (
			id int unsigned auto_increment primary key,
			keyword_id int unsigned,
			foreign key(keyword_id) references keywords(id),
			location_id int unsigned,
			foreign key(location_id) references locations(id),
			advertiser_id int unsigned,
			foreign key(advertiser_id) references advertisers(id),
			unique(keyword_id, location_id, advertiser_id)
		) engine = InnoDB");
}



sub get_locations {
	my @locations;

	my $select_res = $dbh->selectall_arrayref("select id, ip_address from locations");
	foreach my $r (@$select_res) {
		push @locations, {
			id => $$r[0],
			ip_address => $$r[1],
		};
	}
	return \@locations;
}



sub get_keywords {
	my ($l) = @_;

	my @keywords;
	my $select_res = $dbh->selectall_arrayref("select
			k.id,
			k.keyword
		from keywords k
			left join done d on k.id = d.keyword_id and d.location_id = ?
			where d.keyword_id is null
			limit 10", undef, $$l{id});
	foreach my $r (@$select_res) {
		push @keywords, {
			id => $$r[0],
			keyword => $$r[1],
		};
	}
	return \@keywords;
}



sub get_url {
	my ($url, $k, $ek, $page) = @_;

	my $encoded_url = $url . $ek;
	$encoded_url .= "&page=$page" if $page;
	my $tries_left = 3;
	do {
		say "# getting $url|$k|page: $page";
		my $resp = $ua->get($encoded_url);
		return $resp->decoded_content if $resp->is_success;
		return if $resp->code == 404 or $resp->code == 403;
		die; # FIXME
		--$tries_left;
		say "# get_url: resp is NOT success: ", $resp->code;
		sleep 300 if $tries_left;
	} while ($tries_left);
	die "was not able to handle some error during download";
}



sub process_page {
	my ($content_ref, $data) = @_;

	my (@banners) = $$content_ref =~ /bannerData\[\d+\] = {(.+?)}/msg;
	die "unexpected page format" unless @banners;
	foreach my $block (@banners) {
		my $bn;
		foreach my $line (split /\n/, $block) {
			next if $line =~ /^\s*$/;
			my ($key, $value) = split /': /, $line;
			$key =~ s/^\s*'//g;
			next if $key eq 'href' or $key eq 'vcard' or $key eq 'body' or $key eq 'geoId';
			$value =~ s/^'|(',|,)$//g;
			$$bn{$key} = $value;
		}
		push @$data, $bn;
		dump_data $bn, 'banner' if $opts{debug};
	}
}



sub insert_data {
	my ($data) = @_;

	my %ids;
	foreach my $record (@$data) {
		my ($id) = $dbh->selectrow_array("select id from advertisers where domain = ?", undef, $$record{domain});
		unless ($id) {
			$dbh->do("insert into advertisers ("
				. (join ',', keys %$record)
				. ") values ("
				. (join ',', map { '?' } keys %$record)
				. ")",
				undef, values %$record);
			$id = $dbh->last_insert_id(undef, undef, undef, undef);
		}
		$ids{$id} = 1;
	}
	return \%ids;
}



sub process_location {
	my ($l) = @_;

	my $keywords = get_keywords $l;
	die "all keywords are processed" unless @$keywords;
	foreach my $k (@$keywords) {
		my $ek = url_encode $$k{keyword};
		my ($url, @data);
		if ($$l{ip_address} eq '62.149.16.37') {
			$url = 'https://direct.yandex.ru/search?text=';
		} else {
			# TODO: proxy support
			# $url = $$l{ip_address} . /search?text=';
		}
		my ($page, $next);
		do {
			$next = 0;
			my $skip = 0;
			my $content = get_url $url, $$k{keyword}, $ek, $page;
			++$page unless $page;
			if ($content =~ /name="captcha_code"/) {
				die "captcha required";
			} elsif ($content =~ />Ничего не найдено</) {
				say "# nothing found" if $opts{debug};
				$skip = 1;
			} elsif ($content =~ /class="b-pager__next"/) {
				$next = 1;
				++$page;
			}
			process_page \$content, \@data unless $skip;
		} while ($next);

		$dbh->begin_work;
		my $ids = insert_data \@data;
		foreach my $id (keys %$ids) {
			say "# l.id: $$l{id}  k.id: $$k{id}  a.id: $id" if $opts{debug};
			$dbh->do("insert into results (location_id, keyword_id, advertiser_id) values ($$l{id}, $$k{id}, $id)");
		}
		$dbh->do("insert into done (location_id, keyword_id) values (?, ?)", undef, $$l{id}, $$k{id});
		$dbh->commit;
	}
}



# here we start
#
dump_data \%opts, 'options' if $opts{debug};
say "# started: ", strftime "%Y-%m-%d %H:%M:%S", localtime;
create_tables;

my $locations = get_locations;
while (1) {
	my $l = $$locations[int rand @$locations];
	process_location $l;
}

say "# exit";
exit;



