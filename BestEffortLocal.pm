package BestEffortLocal;
use parent 'Basic';
use strict;
use warnings;

use Data::Dumper;
use List::Util qw(min sum);

use ProcessorRange;

sub new {
	my ($class, $platform) = @_;

	my $self = {
		platform => $platform,
	};

	bless $self, $class;
	return $self;
}

sub reduce {
	my ($self, $job, $left_processors) = @_;

	my @remaining_ranges;
	my $used_clusters_number = 0;
	my $current_cluster;

	my $target_number = $job->requested_cpus();

	my @clusters = $self->{platform}->job_processors_in_clusters($left_processors);
	my @sorted_clusters = sort {cluster_size($b) <=> cluster_size($a)} (@clusters);

	for my $cluster (@sorted_clusters) {
		for my $pair (@{$cluster}) {
			my ($start, $end) = @{$pair};
			my $available_processors = $end - $start + 1;
			my $taking = min($target_number, $available_processors);

			push @remaining_ranges, [$start, $start + $taking - 1];
			$target_number -= $taking;
			last if $target_number == 0;
		}

		last if $target_number == 0;
	}

	$left_processors->affect_ranges(ProcessorRange::sort_and_fuse_contiguous_ranges(\@remaining_ranges));

	return;
}

sub cluster_size {
	my ($cluster) = @_;

	return sum map {$_->[1] - $_->[0] + 1} (@{$cluster});
}

1;

