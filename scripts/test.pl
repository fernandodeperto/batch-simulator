#!/usr/bin/env perl
use strict;
use warnings;

use Log::Log4perl qw(get_logger);

use Trace;
use Backfilling;

my ($trace_file, $cpus_number) = @ARGV;
my $cluster_size = 16;

Log::Log4perl::init('log4perl.conf');
my $logger = get_logger();

$logger->info('reading trace');
my $trace = Trace->new_from_swf($trace_file);

$logger->info('running scheduler');
my $schedule = Backfilling->new($trace, $cpus_number, $cluster_size, BASIC);
$schedule->run();
#$schedule->tycat();

#$logger->debug("$jobs_number $cpus_number " . $schedule->{schedule_time});
$logger->info('done');

