package BestEffortContiguous;
use parent 'Basic';
use strict;
use warnings;

use Data::Dumper;
use List::Util qw(min);

use ProcessorRange;

sub new {
	my ($class) = @_;

	my $self = {};

	bless $self, $class;
	return $self;
}

sub reduce {
	my ($self, $job, $left_processors) = @_;

	my $target_number = $job->requested_cpus();
	my @remaining_ranges;
	my @sorted_pairs = sort { $b->[1] - $b->[0] <=> $a->[1] - $a->[0] } $left_processors->pairs();

	for my $pair (@sorted_pairs) {
		my ($start, $end) = @{$pair};
		my $available_processors = $end + 1 - $start;
		my $taking = min($target_number, $available_processors);

		push @remaining_ranges, [$start, $start + $taking - 1];

		$target_number -= $taking;
		last if $target_number == 0;
	}

	$left_processors->affect_ranges(ProcessorRange::sort_and_fuse_contiguous_ranges(\@remaining_ranges));

	return;
}

1;

