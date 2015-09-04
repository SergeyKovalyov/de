#!/usr/bin/env perl
#
# Author: Sergey Kovalyov (sergey.kovalyov@gmail.com)
#
use common::sense;
use Email::Stuffer;
use DBI;

my $dbh = DBI->connect("dbi:mysql:de") or die "Cannot connect: $DBI::errstr";
$dbh->{PrintError} = 0;
$dbh->{RaiseError} = 1;



sub send_mail {
	my ($params) = @_;
	
	my ($count) = $dbh->selectrow_array("select count(*) from advertisers");
	my $msg = "$count domains in the main DB\n\n";
	$msg .= join "\n", sort map { join "\t", @$_{qw/domain phone emails/} } @{$$params{list}};
	utf8::decode $msg;
	my $email = new Email::Stuffer;
	$email->to($$params{to})
		->cc($$params{cc})
		->from($$params{from})
		->subject('В работу')
		->text_body($msg, encoding => '8bit', format => undef)
		->send_or_die;
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

