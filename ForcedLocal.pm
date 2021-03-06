package ForcedLocal;
use parent 'BestEffortLocal';
use strict;
use warnings;

use Data::Dumper;
use POSIX qw(ceil);
use List::Util qw(min sum);

sub new {
	my ($class, @remaining_parameters) = @_;

	my $self = $class->SUPER::new(@remaining_parameters);

	bless $self, $class;
	return $self;
}

sub reduce {
	my ($self, $job, $left_processors) = @_;

	my @remaining_ranges;

	my $target_number = $job->requested_cpus();
	my $used_clusters_number = 0;
	my $current_cluster;

	my @clusters = $self->{platform}->job_processors_in_clusters($left_processors);
	my @sorted_clusters = sort {BestEffortLocal::cluster_size($b) <=> BestEffortLocal::cluster_size($a)} (@clusters);

	my $target_clusters_number = ceil($target_number/$self->{platform}->cluster_size());

	for my $cluster (@sorted_clusters) {
		for my $pair (@{$cluster}) {
			my ($start, $end) = @{$pair};
			my $available_processors = $end - $start + 1;
			my $taking = min($target_number, $available_processors);

			push @remaining_ranges, [$start, $start + $taking - 1];
			$target_number -= $taking;
			last if $target_number == 0;
		}

		$used_clusters_number++;

		if (($used_clusters_number == $target_clusters_number) and ($target_number > 0)) {
			$left_processors->remove_all();
			return;
		}

		last if $target_number == 0;
	}

	if (@remaining_ranges) {
		$left_processors->affect_ranges(ProcessorRange::sort_and_fuse_contiguous_ranges(\@remaining_ranges));
	} else {
		$left_processors->remove_all();
	}
	return;
}

1;

