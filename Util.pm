package Util;
use strict;
use warnings;

use Exporter qw(import);
use Data::Dumper qw(Dumper);

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
	get_benchmark_data
	get_comm_data
);

our $config;

use constant {
	FALSE => 0,
	TRUE => 1
};

sub git_tree_dirty {
	my ($git_path) = @_;

	$git_path = '.' unless defined $git_path;

	my $git_branch = `git -C $git_path symbolic-ref --short HEAD`;
	chomp $git_branch;
	return system('git diff-files --quiet');
}

sub git_desribe {
	my ($git_path) = @_;

	$git_path = '.' unless defined $git_path;

	my $git_version = `git -C $git_path describe --tags --dirty`;
	chomp $git_version;
	return $git_version;
}

sub git_version {
	my ($git_path) = @_;

	$git_path = '.' unless defined $git_path;

	my $git_version = `git -C $git_path rev-parse --short HEAD`;
	chomp $git_version;
	return $git_version;
}

sub float_equal {
	my ($a, $b, $precision) = @_;

	$precision = 6 unless defined $precision;

	#return sprintf("%.${precision}g", $a) eq sprintf("%.${precision}g", $b);
	return (abs($a - $b) < 10 ** -$precision);
}

sub get_benchmark_data {
	my ($benchmark_filename) = @_;

	open(my $benchmark_file, '<', $benchmark_filename) or die 'unable to open the benchmark data file';

	my $header = <$benchmark_file>;
	chomp $header;
	my @header_parts = split(' ', $header);

	my %benchmark_data;

	while (defined(my $line = <$benchmark_file>)) {
		my @line_parts = split(' ', $line);

		$benchmark_data{$line_parts[0]}->{$header_parts[$_]} = $line_parts[$_] for 1..3;
	}

	return %benchmark_data if wantarray;
	return \%benchmark_data;
}

sub get_comm_data {
	my ($comm_data_filename) = @_;

	open(my $comm_data_file, '<', $comm_data_filename) or die 'unable to open comm data file';

	my $comm_data_header = <$comm_data_file>;
	chomp $comm_data_header;
	my @comm_data_header_parts = split(' ', $comm_data_header);

	my %comm_data;

	while (defined(my $line = <$comm_data_file>)) {
		chomp $line;

		my @line_parts = split(' ', $line);

		$comm_data{$line_parts[0]}->{$comm_data_header_parts[$_]} = $line_parts[$_] for 1..3;
	}

	return %comm_data if wantarray;
	return \%comm_data;
}

1;
