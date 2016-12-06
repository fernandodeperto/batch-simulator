#!/usr/bin/env perl
use strict;
use warnings;

use Data::Dumper qw(Dumper);

use threads;

use Trace;
use Backfilling;
use Basic;
use BestEffortContiguous;
use ForcedContiguous;
use BestEffortLocal;
use ForcedLocal;
use BestEffortPlatform;
use ForcedPlatform;

my ($trace_file) = @ARGV;

my @platform_levels = (1, 2, 4, 8, 64);
my $cluster_size = $platform_levels[$#platform_levels]/$platform_levels[$#platform_levels - 1];
my $delay = 10;
my $socket_file = '/tmp/socket';
my $json_file = '/tmp/json';
my $cpus_number = $platform_levels[$#platform_levels];
my $comm_factor = '4e6';
my $comp_factor = '3e10';
my $batsim = '../batsim/build/batsim';
my $platform_file = '/tmp/platform';

my $platform = Platform->new(\@platform_levels);
$platform->build_platform_xml();
$platform->save_platform_xml($platform_file);

my $reduction_algorithm = Basic->new();

my $trace = Trace->new_from_swf($trace_file);
$trace->save_json($json_file, $cpus_number, $comm_factor, $comp_factor);

my $batsim_thread = threads->create(\&run_batsim, $json_file);
my $schedule = Backfilling->new_simulation($reduction_algorithm, $delay, $socket_file, $json_file, $cluster_size);

$schedule->run();
$batsim_thread->join();

my @results = (
	$schedule->cmax(),
	$schedule->contiguous_jobs_number(),
	$schedule->local_jobs_number(),
	$schedule->locality_factor(),
	$schedule->run_time(),
);

print STDOUT join(' ', @results);

sub run_batsim {
	my $batsim_result =  `$batsim -s $socket_file -m'master_host0' -- $platform_file $json_file 2>/dev/null`;
	#print "$batsim -s $socket_file -m'master_host0' -- $platform_file $json_file\n";
	#my $batsim_result =  `$batsim -s $socket_file -m'master_host0' -- $platform_file $json_file`;
	return $batsim_result;
}
