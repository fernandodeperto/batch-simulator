package Util;
use strict;
use warnings;

use Exporter qw(import);

our @EXPORT_OK = qw(
	FALSE
	TRUE
	git_tree_dirty
	git_version
	float_equal
	float_precision
	git_tree_dirty
	git_version
	$config
);


our $config;

sub git_tree_dirty {
	my $git_path = shift;

	$git_path = '.' unless defined $git_path;

	my $git_branch = `git -C $git_path symbolic-ref --short HEAD`;
	chomp $git_branch;
	return system('git diff-files --quiet');
}

sub git_desribe {
	my $git_path = shift;

	$git_path = '.' unless defined $git_path;

	my $git_version = `git -C $git_path describe --tags --dirty`;
	chomp $git_version;
	return $git_version;
}

sub git_version {
	my $git_path = shift;

	$git_path = '.' unless defined $git_path;

	my $git_version = `git -C $git_path rev-parse --short HEAD`;
	chomp $git_version;
	return $git_version;
}

use constant {
	FALSE => 0,
	TRUE => 1
};

sub float_equal {
	my $a = shift;
	my $b = shift;
	my $precision = shift;

	$precision = 6 unless defined $precision;

	#return sprintf("%.${precision}g", $a) eq sprintf("%.${precision}g", $b);
	return (abs($a - $b) < 10 ** -$precision);
}

1;
