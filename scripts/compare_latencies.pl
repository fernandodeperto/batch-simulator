#!/usr/bin/env perl
use strict;
use warnings;

use Log::Log4perl qw(get_logger :no_extra_logdie_message);
use Data::Dumper;

Log::Log4perl::init('log4perl.conf');
my $logger = get_logger('compare_latencies');

my ($execution_id, $required_cpus) = @ARGV;

my $threads_number = 6;
my @benchmarks;
my @latencies = (1..50);
my @permutations;

my $benchmark = "benchmarks/cg.B.$required_cpus";
my $base_path = "experiment/combinations/combinations-$execution_id";
my $platform_file = "$base_path/platform.xml";
my $tmp_platform_file = '/tmp/platform.xml';
my $permutations_file = "$base_path/permutations";
my $hosts_file = '/tmp/hosts';
my $results_file = "$base_path/compare_latencies-$execution_id.csv";

$logger->info("running execution id $execution_id, $required_cpus required cpus");
$logger->info("running benchmarks: @benchmarks");

# Refuse to start if the directory or one of the files doesn't exist
$logger->logdie("experiment directory doesn't exist at $base_path") unless (-d $base_path);
$logger->logdie("platform file doesn't exist at $platform_file") unless (-e $platform_file);
$logger->logdie("permutations file doesn't exist at $permutations_file") unless (-e $permutations_file);

open(my $permutations_fd, '<', $permutations_file) or $logger->logdie("permutation file doesn't exist at $permutations_file");

while (defined(my $permutation = <$permutations_fd>)) {
	chomp($permutation);
	push @permutations, $permutation;
}

open(my $results_fd, '>', $results_file) or $logger->logdie("unable to create file $results_file");
print $results_fd "LATENCY "  . join(' ', map { "p$_" } (0..$#permutations)) . "\n";

for my $latency (@latencies) {
	my @latency_results;

	write_platform_file($latency);

	for my $permutation (@permutations) {
		$logger->info("runing latency $latency permutation $permutation");

		write_host_file($permutation);

		#$logger->debug("./scripts/smpi/smpireplay.sh $required_cpus $tmp_platform_file $hosts_file $benchmark");
		my $result = `./scripts/smpi/smpireplay.sh $required_cpus $tmp_platform_file $hosts_file $benchmark`;
		$logger->logdie("error running benchmark") unless ($result =~ /Simulation time (\d*\.\d*)/);

		push @latency_results, $1;
	}

	print $results_fd "$latency " . join(' ', @latency_results) . "\n";

}

sub write_host_file {
	my $permutation = shift;

	my @permutation_parts = split('-', $permutation);

	open(my $fd, '>', $hosts_file);
	print $fd "$_\n" for (@permutation_parts);
	return;
}

sub get_log_file {
	return "log/compare_latencies.log";
}

sub write_platform_file {
	my $latency = shift;
	my $result = `sed -e 's/EXTERNAL_LATENCY/$latency.0E-4/' $platform_file > $tmp_platform_file`;
}


