package ForcedContiguous;
use parent 'Basic';
use strict;
use warnings;

use Data::Dumper;

use ProcessorRange;

sub new {
	my $class = shift;

	my $self = {};

	bless $self, $class;
	return $self;
}

sub reduce {
	my $self = shift;
	my $job = shift;
	my $left_processors = shift;
	my @remaining_ranges;

	my $target_number = $job->requested_cpus();

	$left_processors->ranges_loop(
		sub {
			my ($start, $end) = @_;
			my $available_processors = $end + 1 - $start;

			return 1 if ($available_processors < $target_number);

			push @remaining_ranges, [$start, $start + $target_number - 1];
			return 0;
		},
	);

	if (@remaining_ranges) {
		$left_processors->affect_ranges(ProcessorRange::sort_and_fuse_contiguous_ranges(\@remaining_ranges));
	} else {
		$left_processors->remove_all();
	}

	return;
}

1;

