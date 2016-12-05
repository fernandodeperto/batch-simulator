package ForcedPlatform;
use parent 'BestEffortPlatform';
use strict;
use warnings;

use Data::Dumper;
use Switch;

use ProcessorRange;
use BestEffortPlatform qw(DEFAULT SMALLEST_FIRST BIGGEST_FIRST);

sub new {
	my $class = shift;
	my $self = $class->SUPER::new(@_);

	bless $self, $class;
	return $self;
}

sub reduce {
	my $self = shift;
	my $job = shift;
	my $left_processors = shift;

	my $available_cpus = $self->{platform}->available_cpus_in_clusters($left_processors);
	my $cpus_structure = $self->{platform}->build_structure($available_cpus);
	my $chosen_ranges = $self->choose_cpus($cpus_structure, $job->requested_cpus());

	if (defined $chosen_ranges) {
		$left_processors->affect_ranges(ProcessorRange::sort_and_fuse_contiguous_ranges($chosen_ranges));
	} else {
		$left_processors->remove_all();
	}

	return;
}

sub choose_cpus {
	my $self = shift;
	my $cpus_structure = shift;
	my $target_number = shift;

	my @suitable_levels = grep {$_->[0]->{total_original_size} >= $target_number} (@{$cpus_structure});

	# The first level is the only one that can be used since this is a
	# forced variant
	my @sorted_blocks;
	switch ($self->{mode}) {
		case SMALLEST_FIRST {
			@sorted_blocks = sort { $a->{total_size} <=> $b->{total_size} } (@{$suitable_levels[0]});
		}

		case BIGGEST_FIRST {
			@sorted_blocks = sort { $b->{total_size} <=> $a->{total_size} } (@{$suitable_levels[0]});
		}

		else {
			@sorted_blocks = @{$suitable_levels[0]};
		}
	}

	my $chosen_block;
	for my $cpus_block (@sorted_blocks) {
		if ($cpus_block->{total_size} >= $target_number) {
			$chosen_block = $cpus_block;
			last;
		}
	}

	# If this is undefined means there are no minimum sized blocks that
	# have enough available CPUs for the job
	return unless (defined $chosen_block);

	my @chosen_ranges;
	my $range_start = shift @{$chosen_block->{cpus}};
	my $range_end = $range_start;
	my $taken_cpus = 0;

	while ($taken_cpus < $target_number) {
		my $cpu_number = shift @{$chosen_block->{cpus}};

		while ((defined $cpu_number) and ($cpu_number == $range_end + 1)
				and ($taken_cpus + $range_end - $range_start + 1 < $target_number)) {
			$range_end = $cpu_number;
			$cpu_number = shift @{$chosen_block->{cpus}};
		}

		push @chosen_ranges, [$range_start, $range_end];
		$taken_cpus += $range_end - $range_start + 1;
		$range_start = $cpu_number;
		$range_end = $range_start;
	}

	return \@chosen_ranges;
}




1;

