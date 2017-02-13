package Util;
use strict;
use warnings;

use Exporter qw(import);
use Data::Dumper qw(Dumper);

our @EXPORT_OK = qw(
	FALSE
	TRUE
	float_equal
	$config
);

our $config;

use constant {
	FALSE => 0,
	TRUE => 1
};

sub float_equal {
	my ($a, $b, $precision) = @_;

	$precision = 6 unless defined $precision;

	#return sprintf("%.${precision}g", $a) eq sprintf("%.${precision}g", $b);
	return (abs($a - $b) < 10 ** -$precision);
}

1;
