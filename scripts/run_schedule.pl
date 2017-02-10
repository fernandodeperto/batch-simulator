#!/usr/bin/env perl
use strict;
use warnings;

use Data::Dumper qw(Dumper);
use Config::Simple;
use Switch;

use Util qw($config get_benchmark_data get_comm_data);
use Trace;
use Backfilling;
use Basic;
use BestEffortContiguous;
use ForcedContiguous;
use BestEffortLocal;
use ForcedLocal;
use BestEffortPlatform qw(SMALLEST_FIRST BIGGEST_FIRST);
use ForcedPlatform;

$config = Config::Simple->new('test.conf');

my $platform_name = $config->param('parameters.platform_name');
my @platform_levels = $config->param("$platform_name.platform_levels");

my $platform = Platform->new(\@platform_levels);

my $traces_path = $config->param('paths.traces');
my $swf_filename = $config->param("$platform_name.swf_file");
my $jobs_number = $config->param('parameters.jobs_number');

my $trace = Trace->new_from_swf("$traces_path/$swf_filename");
$trace->remove_large_jobs($platform->processors_number());
$trace->reset_jobs_numbers();
$trace->fix_submit_times();
$trace->keep_first_jobs($jobs_number) if defined $jobs_number;

my $reduction_algorithm;

switch ($config->param('parameters.variant_name')) {
	case 'basic' {
		$reduction_algorithm = Basic->new();
	}

	case 'becont' {
		$reduction_algorithm = BestEffortContiguous->new();
	}

	case 'beloc' {
		$reduction_algorithm = BestEffortLocal->new($platform);
	}

	case 'beplat' {
		$reduction_algorithm = BestEffortPlatform->new($platform);
	}

	case 'cont' {
		$reduction_algorithm = ForcedContiguous->new();
	}

	case 'loc' {
		$reduction_algorithm = ForcedLocal->new($platform);
	}

	case 'plat' {
		$reduction_algorithm = ForcedPlatform->new($platform);
	}
}

my $penalty_function = $config->param('parameters.penalty_function');
my $penalty_function_factor = $config->param('parameters.penalty_function_' . $penalty_function . '_factor');

my $schedule = Backfilling->new(
	$reduction_algorithm,
	$platform,
	$trace,
	$penalty_function,
	$penalty_function_factor,
);
$schedule->run();

my @results = (
	$schedule->cmax(),
	#$schedule->contiguous_jobs_number(),
	#$schedule->local_jobs_number(),
	#$schedule->locality_factor(),
	#$schedule->stretch_sum_of_squares(),
	#$schedule->stretch_with_cpus_squared(),
	#$schedule->run_time(),
);

print STDOUT join(' ', @results) . "\n";

