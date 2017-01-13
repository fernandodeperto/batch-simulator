package BestEffortPlatform;
use parent 'Basic';
use strict;
use warnings;

use Data::Dumper;
use Switch;

use ProcessorRange;
use Platform;

require Exporter;
our @ISA = qw(Exporter);

use constant {
	DEFAULT => 0,
	SMALLEST_FIRST => 1,
	BIGGEST_FIRST => 2,
};

our @EXPORT_OK = qw(
	DEFAULT
	SMALLEST_FIRST
	BIGGEST_FIRST
);

sub new {
	my ($class, $platform, @remaining_parameters) = @_;

	my %args = @remaining_parameters;
	my $mode = $args{mode} or DEFAULT;

	my $self = {
		platform => $platform,
		mode => $mode,
	};

	bless $self, $class;
	return $self;
}

sub reduce {
	my ($self, $job, $left_processors) = @_;

	my $available_cpus = $self->{platform}->available_cpus_in_clusters($left_processors);
	my $cpus_structure = $self->{platform}->build_structure($available_cpus);
	my $chosen_ranges = $self->choose_cpus($cpus_structure, $job->requested_cpus());

	$left_processors->affect_ranges(ProcessorRange::sort_and_fuse_contiguous_ranges($chosen_ranges));

	return;
}

sub choose_cpus {
	my ($self, $cpus_structure, $target_number) = @_;

	my @suitable_levels = grep {$_->[0]->{total_original_size} >= $target_number} (@{$cpus_structure});

	my $chosen_block;

	for my $structure_level (@suitable_levels) {
		my @sorted_blocks;

		switch ($self->{mode}) {
			case SMALLEST_FIRST {
				@sorted_blocks = sort {$a->{total_size} <=> $b->{total_size}} (@{$structure_level});
			}

			case BIGGEST_FIRST {
				@sorted_blocks = sort {$b->{total_size} <=> $a->{total_size}} (@{$structure_level});
			}

			else {
				@sorted_blocks = @{$structure_level};
			}
		}

		for my $cpus_block (@sorted_blocks) {
			if ($cpus_block->{total_size} >= $target_number) {
				$chosen_block = $cpus_block;
				last;
			}
		}

		last if defined $chosen_block;
	}

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

	return @chosen_ranges if wantarray;
	return \@chosen_ranges;
}

1;

