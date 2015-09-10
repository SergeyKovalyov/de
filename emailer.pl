#!/usr/bin/env perl
#
# Author: Sergey Kovalyov (sergey.kovalyov@gmail.com)
#
use common::sense;
use autodie;
use Email::Stuffer;
use DBI;

my $dbh = DBI->connect("dbi:mysql:de") or die "Cannot connect: $DBI::errstr";
$dbh->{PrintError} = 0;
$dbh->{RaiseError} = 1;



sub send_mail {
	my ($params) = @_;
	
	my $file = 'list.csv';
	my ($count) = $dbh->selectrow_array("select count(*) from advertisers");
	my $msg = "$count domains in the main DB\n\n";

	open my $fh, '>', $file;
	say $fh 'www;phone1;phone2;phone3;email1;email2;email3;email4;email5';
	foreach my $r (@{ $params->{list} }) {
		my @emails = split / /, $r->{emails};
		if (@emails > 5) {
			splice @emails, 5;
		} else {
			push @emails, '' while @emails < 5;
		}
		my $line = join ';', @$r{qw/domain phone/}, undef, undef, @emails;
		say $fh $line;
	}
	close $fh;

	my $email = new Email::Stuffer;
	$email->to($$params{to})
		->cc($$params{cc})
		->from($$params{from})
		->subject('В работу')
		->text_body($msg)
		->attach_file($file)
		->send_or_die;
	unlink $file;
}



sub mark_records {
	my ($list) = @_;

	my $st = "update advertisers set sent = 1 where id in ("
		. (join ',', map { $$_{id} } @$list)
		. ")";
	$dbh->do($st);
}



# here we start
#
my $list = $dbh->selectall_arrayref("select id, domain, phone, emails from advertisers
		where sent = 0 and checked = 1", { Slice => {} });
if (@$list) {
	send_mail {
		to   => 'tikhonova.e@adpremium-team.ru',
		cc   => 'dmitry@eremeev.ru, wise@nowhere.kiev.ua',
		from => 'dmitry@eremeev.ru',
		list => $list,
	};
	mark_records $list;
}
exit;

