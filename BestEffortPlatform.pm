package BestEffortPlatform;
use parent 'Basic';
use strict;
use warnings;

use lib 'ProcessorRange/blib/lib', 'ProcessorRange/blib/arch';
use ProcessorRange;

use Data::Dumper;

sub new {
	my $class = shift;
	my $platform_levels = shift;

	my $self = {
		platform_levels => $platform_levels,
	};

	bless $self, $class;
	return $self;
}

sub reduce {
	my $self = shift;
	my $target_number = shift;
	my $left_processors = shift;

	my $cluster_size = $self->{platform_levels}->[$#{$self->{platform_levels}}]/$self->{platform_levels}->[$#{$self->{platform_levels}} - 1];

	my $available_cpus = $left_processors->available_cpus_in_clusters($cluster_size);
	my $platform = Platform->new($self->{platform_levels});
	my $cpus_structure = $platform->build_structure($available_cpus);
	my $chosen_ranges = choose_cpus($cpus_structure, $target_number);

	$left_processors->affect_ranges(ProcessorRange::sort_and_fuse_contiguous_ranges($chosen_ranges));
	return;
}

sub choose_cpus {
	my $cpus_structure = shift;
	my $target_number = shift;

	my $chosen_block;

	# Find the first block with enough CPUs for the job
	for my $structure_level (@{$cpus_structure}) {
		for my $cpus_block (@{$structure_level}) {
			if ($cpus_block->{total_size} >= $target_number) {
				$chosen_block = $cpus_block;
				last;
			}
		}
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

	return \@chosen_ranges;
}

1;

