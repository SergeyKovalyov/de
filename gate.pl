#!/usr/bin/perl
#
# CGI script works as proxy on virtual hosting sites 
#
use common::sense;
use CGI qw/:standard/;
use LWP::UserAgent;

my $ua = new LWP::UserAgent(agent => 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:36.0) Gecko/20100101 Firefox/36.0');

sub get_url {
	my ($url) = @_;

	my $tries_left = 3;
	do {
		my $resp = $ua->get($url);
		if ($resp->is_success) {
			return $resp->decoded_content;
		} elsif ($resp->code == 404 or $resp->code == 403) {
			return;
		} else {
			$tries_left--;
			say "# get_url: resp is NOT success: ", $resp->code;
			sleep 300 if $tries_left;
		}
	} while ($tries_left);
	say "# was not able to handle some error during download";
	exit;
}

# here we start
#
print header(
	-type => 'text/html',
	-charset => 'utf-8',
);
my $k = param('text');
my $p = param('page');
my $url = "https://direct.yandex.ru/search?text=$k";
$url .= "&page=$p" if $p;

my $content = get_url $url;
if ($content) {
	print $content;
} else {
	print "no content";
}

exit;

