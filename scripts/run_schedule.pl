#!/usr/bin/env perl
use strict;
use warnings;

use Data::Dumper qw(Dumper);
use Log::Log4perl qw(get_logger);

use Util qw(git_version);
use Trace;
use Backfilling;
use Basic;
use BestEffortContiguous;
use ForcedContiguous;
use BestEffortLocal;
use ForcedLocal;
use BestEffortPlatform qw(SMALLEST_FIRST BIGGEST_FIRST);
use ForcedPlatform;

my ($trace_file, $jobs_number) = @ARGV;

my @platform_levels = (1, 4, 8, 512);
my @platform_speedup = (1.00, 8.00, 64.00);

my $platform = Platform->new(\@platform_levels);
$platform->set_speedup(\@platform_speedup);

my $trace_original = Trace->new_from_swf($trace_file);
$trace_original->remove_large_jobs($platform->processors_number());
my $trace = Trace->new_from_trace($trace_original, $jobs_number);
#my $trace = Trace->new_from_swf($trace_file);
$trace->reset_jobs_numbers();
$trace->reset_submit_times();
#$trace->fix_submit_times();
#$trace->keep_first_jobs($jobs_number) if defined $jobs_number;

my $reduction_algorithm = BestEffortPlatform->new($platform);

my $schedule = Backfilling->new($reduction_algorithm, $platform, $trace);
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

$schedule->save_svg('svg/run_schedule.svg');

sub get_log_file {
	return 'log/run_schedule.log';
}
