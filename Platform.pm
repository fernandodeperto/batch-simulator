package Platform;
use strict;
use warnings;

use Data::Dumper;
use List::Util qw(min max sum);
use POSIX;
use XML::Smart;
use Carp;

use Tree;
use ProcessorRange;

# Constructors and helper functions

sub new {
	my ($class, $levels) = @_;

	my $self = {
		levels => [@{$levels}],
	};

	bless $self, $class;
	return $self;
}

sub processors_number {
	my ($self) = @_;
	return $self->{levels}->[-1];
}

sub cluster_size {
	my ($self) = @_;
	return $self->{levels}->[-1]/$self->{levels}->[-2];
}

# Tree structure

sub build_tree {
	my ($self, $available_cpus) = @_;

	$self->{root} = $self->_build_tree(0, 0, $available_cpus);

	return;
}

sub _build_tree {
	my ($self, $level, $node, $available_cpus) = @_;

	my $next_level_nodes = $self->{levels}->[$level + 1]/$self->{levels}->[$level];
	my @next_level_nodes_ids = map {$next_level_nodes * $node + $_} (0..($next_level_nodes - 1));

	# last level before the leafs/nodes
	if ($level == $#{$self->{levels}} - 1) {
		my $tree_content = {
			total_size => (defined $available_cpus->[$node]) ? $available_cpus->[$node] : 0,
			nodes => [@next_level_nodes_ids],
			id => $node
		};
		return Tree->new($tree_content);
	}

	my @children = map {$self->_build_tree($level + 1, $_, $available_cpus)} (@next_level_nodes_ids);

	my $total_size = 0;
	$total_size += $_->content()->{total_size} for (@children);

	my $tree_content = {total_size => $total_size, id => $node};
	my $tree = Tree->new($tree_content);

	$tree->children(\@children);

	return $tree;
}

sub choose_cpus {
	my ($self, $requested_cpus) = @_;

	$self->_score($self->{root}, 0, $requested_cpus);


	return $self->_choose_cpus($self->{root}, $requested_cpus);
}

sub _choose_cpus {
	my ($self, $tree, $requested_cpus) = @_;

	# no requested cpus
	return unless $requested_cpus;

	my @children = @{$tree->children()};

	# leaf node/CPU
	return $tree->content()->{id} if (defined $tree->content()->{id});

	my $best_combination = $tree->content()->{$requested_cpus};

	my @combination_parts = split('-', $best_combination->{combination});

	return map {$self->_choose_cpus($_, shift @combination_parts)} (@children);
}

sub _combinations {
	my ($self, $tree, $requested_cpus, $node) = @_;

	my @children = @{$tree->children()};
	my $last_child = $#children;

	# last node
	return $requested_cpus if ($node == $last_child);

	my @remaining_children = @children[($node + 1)..$last_child];
	my $remaining_size = sum (map {$_->content()->{total_size}} (@remaining_children));

	my $minimum_cpus = max(0, $requested_cpus - $remaining_size);
	my $maximum_cpus = min($children[$node]->content()->{total_size}, $requested_cpus);

	my @combinations;

	for my $cpus_number ($minimum_cpus..$maximum_cpus) {
		my @children_combinations = $self->_combinations($tree, $requested_cpus - $cpus_number, $node + 1);
		push @combinations, [$cpus_number, $_] for (@children_combinations);
	}

	return @combinations;
}

sub _score {
	my ($self, $tree, $level, $requested_cpus) = @_;

	# no needed CPUs
	return 0 unless $requested_cpus;

	my $max_depth = $#{$self->{levels}} - 1;

	# leaf/CPU
	return 0 if ($level == $max_depth);

	# best combination already saved
	return $tree->content()->{$requested_cpus}->{score} if (defined $tree->content()->{$requested_cpus});

	my @children = @{$tree->children()};
	my $last_child = $#children;
	my @combinations = $self->_combinations($tree, $requested_cpus, 0);
	my %best_combination = (score => LONG_MAX, combination => '');

	for my $combination (@combinations) {
		my $score = 0;

		for my $child_id (0..$last_child) {
			my $child_size = $children[$child_id]->content()->{total_size};
			my $child_requested_cpus = $combination->[$child_id];

			my $child_score = $self->_score($children[$child_id], $level + 1, $child_requested_cpus);
			$score = max($score, $child_score);
		}

		# add to the score if there is communication between different child nodes
		$score += ($max_depth + 1 - $level) if (max(@{$combination}) < $requested_cpus);

		if ($score < $best_combination{score}) {
			$best_combination{score} = $score;
			$best_combination{combination} = $combination;
		}
	}

	$tree->content()->{$requested_cpus} = \%best_combination;

	return $best_combination{score};
}

sub _score_function_pnorm {
	my ($self, $child_requested_cpus, $requested_cpus, $level) = @_;

	return $child_requested_cpus * ($requested_cpus - $child_requested_cpus) * pow(($#{$self->{levels}} - $level) * 2, $self->{norm});
}

# Linear structure

sub build_structure {
	my ($self, $available_cpus) = @_;

	my $last_level = $#{$self->{levels}} - 1;

	my @cpus_structure;

	for my $level (0..$last_level) {
		$cpus_structure[$level] = [];

		my $nodes_per_block = $self->{levels}->[$last_level]/$self->{levels}->[$last_level - $level];

		for my $block (0..($self->{levels}->[$last_level - $level] - 1)) {
			my $block_content = {
				total_size => 0,
				total_original_size => $self->{levels}->[-1]/$self->{levels}->[$last_level - $level],
				cpus => []
			};

			for my $cluster (($block * $nodes_per_block)..(($block + 1) * $nodes_per_block - 1)) {
				next unless (defined $available_cpus->[$cluster]);

				$block_content->{total_size} += $available_cpus->[$cluster]->{total_size};
				push @{$block_content->{cpus}}, @{$available_cpus->[$cluster]->{cpus}};
			}

			push @{$cpus_structure[$level]}, $block_content;
		}
	}

	return @cpus_structure if wantarray;
	return \@cpus_structure;
}

sub generate_slowdown {
	my ($self, $benchmark, $platform_file, $replay_script) = @_;

	my $hosts_file = '/tmp/hosts';
	my $cpus_number = 2;

	my $last_level = $#{$self->{levels}};

	my @hosts_configs = reverse map {[0, int($self->{levels}->[$last_level]/$self->{levels}->[$_])]} (1..$last_level);

	my @results;

	for my $hosts_config (@hosts_configs) {
		save_hosts_file($hosts_config, $hosts_file);

		my $result = `$replay_script $cpus_number $platform_file $hosts_file $benchmark 2>&1`;

		unless ($result =~ /Simulation time (\d*\.\d*)/) {
			print STDERR "$replay_script $cpus_number $platform_file $hosts_file $benchmark\n";
			print STDERR "$result\n";
			die 'error running benchmark';
		}

		push @results, $1;
	}

	@results = map {$_/$results[0]} (@results);
	$self->{slowdown} = [@results];

	return;
}

sub run_benchmark {
	my ($self, $benchmark, $platform_file, $hosts_config, $script) = @_;

	my $hosts_filename = '/tmp/hosts';
	my $cpus_number = @$hosts_config;

	save_hosts_file($hosts_config, $hosts_filename);

	return `$script $cpus_number $platform_file $hosts_filename $benchmark 2>&1`;
}

sub set_slowdown_from_latencies {
	my ($self, $latencies) = @_;

	@{$self->{slowdown}} = reverse map {$_/$latencies->[-1]} (@{$latencies});

	return;
}

sub save_hosts_file {
	my ($hosts_config, $hosts_file) = @_;

	open(my $file, '>', $hosts_file);
	print $file join("\n", @{$hosts_config}) . "\n";
	close($file);

	return;
}

sub slowdown {
	my ($self, $slowdown) = @_;

	if (defined $slowdown) {
		return $self->{slowdown}->[$slowdown] unless ref $slowdown eq 'ARRAY';

		$self->{slowdown} = [@{$slowdown}];
	}

	return @{$self->{slowdown}};
}

# Platform XML

sub build_platform_xml {
	my ($self, $power, $bandwidths, $latencies) = @_;

	my @platform_parts = @{$self->{levels}};

	my $xml = XML::Smart->new();

	$xml->{platform} = {version => 4};

	# root system
	$xml->{platform}{AS} = {
		id => "AS_Root",
		routing => "Floyd",
	};

	# tree system
	$xml->{platform}{AS}{AS} = {
		id => "AS_Tree",
		routing => "Floyd",
	};

	# push the first router
	push @{$xml->{platform}{AS}{AS}{router}}, {id => "R-0-0"};

	# build levels
	for my $level (1..($#platform_parts - 2)) {
		my $nodes_number = $platform_parts[$level];

		for my $node_number (0..($nodes_number - 1)) {
			push @{$xml->{platform}{AS}{AS}{router}}, {id => "R-$level-$node_number"};

			my $father_node = int $node_number/($platform_parts[$level]/$platform_parts[$level - 1]);

			push @{$xml->{platform}{AS}{AS}{link}}, {
				id => "L-$level-$node_number",
				bandwidth => $bandwidths->[$level - 1],
				latency => $latencies->[$level - 1],
			};

			push @{$xml->{platform}{AS}{AS}{route}}, {
				src => 'R-' . ($level - 1) . "-$father_node",
				dst => "R-$level-$node_number",
				link_ctn => {id => "L-$level-$node_number"},
			};
		}
	}

	# clusters
	for my $cluster (0..($platform_parts[-2] - 1)) {
		push @{$xml->{platform}{AS}{cluster}}, {
			id => "C-$cluster",
			prefix => "",
			suffix => "",
			radical => ($cluster * $self->cluster_size()) . '-' . (($cluster + 1) * $self->cluster_size() - 1),
			speed => $power,
			bw => $bandwidths->[-1],
			lat => $latencies->[-1],
			router_id => "R-$cluster",
		};

		push @{$xml->{platform}{AS}{link}}, {
			id => "L-$cluster",
			bandwidth => $bandwidths->[-2],
			latency => $latencies->[-2],
		};

		my $father_node = int $cluster/($platform_parts[-2]/$platform_parts[-3]);

		push @{$xml->{platform}{AS}{ASroute}}, {
			src => "C-$cluster",
			gw_src => "R-$cluster",
			dst => "AS_Tree",
			gw_dst => 'R-' . ($#platform_parts - 2) . "-$father_node",
			link_ctn => {id => "L-$cluster"},
		}
	}

	$self->{xml} = $xml;

	return;
}

sub save_platform_xml {
	my ($self, $filename) = @_;

	open(my $file, '>', $filename);

	print $file "<?xml version=\'1.0\'?>\n" . "<!DOCTYPE platform SYSTEM \"http://simgrid.gforge.inria.fr/simgrid/simgrid.dtd\">\n" . $self->{xml}->data(noheader => 1, nometagen => 1);

	return;
}

# Platform level measure for jobs

sub job_level_distance {
	my ($self, $assigned_processors) = @_;

	my @used_clusters = $self->job_used_clusters($assigned_processors);
	my $last_level = $#{$self->{levels}};
	my $clusters_number = $self->{levels}->[$last_level - 1];

	# return 1 if there is only one cluster
	return 1 if (@used_clusters == 1);

	for my $level (0..($last_level - 2)) {
		my $clusters_per_side = $clusters_number / $self->{levels}->[$level + 1];
		my $clusters_side = int($used_clusters[0] / $clusters_per_side);

		for my $cluster_id (1..$#used_clusters) {
			if (int($used_clusters[$cluster_id] / $clusters_per_side) != $clusters_side) {
				return $last_level - $level;
			}
		}
	}

	return 1;
}

sub job_minimum_level_distance {
	my ($self, $requested_cpus) = @_;

	my $last_level = $#{$self->{levels}};
	my $minimum_level_distance;

	for my $level (reverse(0..($last_level - 1))) {
		my $cpus_per_level = $self->{levels}->[$last_level] / $self->{levels}->[$level];

		if ($cpus_per_level >= $requested_cpus) {
			return $last_level - $level;
		}
	}
}

sub job_relative_level_distance {
	my ($self, $assigned_processors, $requested_cpus) = @_;

	my $last_level = $#{$self->{levels}};
	my $clusters_number = $self->{levels}->[-2];

	my @used_clusters = $self->job_used_clusters($assigned_processors);

	# return 1 if there is only one cluster
	return 1 if (scalar @used_clusters == 1);

	# compute minimum level distance for the job
	my $minimum_level_distance;
	for my $level (reverse(0..($last_level - 1))) {
		my $cpus_per_level = $self->{levels}->[$last_level] / $self->{levels}->[$level];
		if ($cpus_per_level >= $requested_cpus) {
			$minimum_level_distance = $last_level - $level;
			last;
		}
	}

	for my $level (0..($last_level - 2)) {
		my $clusters_per_side = $clusters_number / $self->{levels}->[$level + 1];
		my $clusters_side = int($used_clusters[0] / $clusters_per_side);

		for my $cluster_id (1..$#used_clusters) {
			if (int($used_clusters[$cluster_id] / $clusters_per_side) != $clusters_side) {
				return ($last_level - $level) / $minimum_level_distance;
			}
		}
	}

	return 1;
}

# Contiguity and locality

sub job_contiguity {
	my ($self, $assigned_processors) = @_;

	my @ranges = $assigned_processors->pairs();

	return 1 if (scalar @ranges == 1);

	return 1 if (scalar @ranges == 2 and $ranges[0]->[0] == 0
	and $ranges[1]->[1] == $self->processors_number() - 1);

	return 0;
}

sub job_contiguity_factor {
	my ($self, $assigned_processors) = @_;

	my @ranges = $assigned_processors->pairs();

	return 1 if (scalar @ranges == 2 and $ranges[0]->[0] == 0
	and $ranges[1]->[1] == $self->processors_number() - 1);

	return scalar @ranges;
}

sub job_locality {
	my ($self, $assigned_processors) = @_;

	my @used_clusters = $self->job_used_clusters($assigned_processors);

	return 1 if (@used_clusters == ceil($assigned_processors->size() / $self->cluster_size()));
	return 0;
}

sub job_used_clusters {
	my ($self, $assigned_processors) = @_;

	my %used_clusters;

	$assigned_processors->ranges_loop(
		sub {
			my ($start, $end) = @_;

			my $start_cluster = floor($start/$self->cluster_size());
			my $end_cluster = floor($end/$self->cluster_size());

			$used_clusters{$_} = 1 for ($start_cluster..$end_cluster);

			return 1;

		}
	);

	return keys %used_clusters;
}

sub job_processors_in_clusters {
	my ($self, $assigned_processors) = @_;

	my @clusters;
	my $current_cluster;

	$assigned_processors->ranges_loop(
		sub {
			my ($start, $end) = @_;

			my $start_cluster = floor($start/$self->cluster_size());
			my $end_cluster = floor($end/$self->cluster_size());

			for my $cluster ($start_cluster..$end_cluster) {
				my $start_point_in_cluster = max($start, $cluster*$self->cluster_size());
				my $end_point_in_cluster = min($end, ($cluster+1)*$self->cluster_size()-1);

				push @clusters, [] unless defined $current_cluster and $cluster == $current_cluster;
				$current_cluster = $cluster;

				push @{$clusters[-1]}, [$start_point_in_cluster, $end_point_in_cluster];
			}

			return 1;
		}
	);

	return @clusters;
}

sub available_cpus_in_clusters {
	my ($self, $processors) = @_;

	my @available_cpus;

	$processors->ranges_loop(
		sub {
			my ($start, $end) = @_;

			my $start_cluster = floor($start/$self->cluster_size());
			my $end_cluster = floor($end/$self->cluster_size());

			for my $cluster ($start_cluster..$end_cluster) {
				my $start_point_in_cluster = max($start, $cluster * $self->cluster_size());
				my $end_point_in_cluster = min($end, ($cluster + 1) * $self->cluster_size() - 1);

				$available_cpus[$cluster] = {
					total_size => 0,
					cpus => []
				} unless (defined $available_cpus[$cluster]);

				$available_cpus[$cluster]->{total_size} += $end_point_in_cluster - $start_point_in_cluster + 1;
				push @{$available_cpus[$cluster]->{cpus}}, ($start_point_in_cluster..$end_point_in_cluster);
			}

			return 1;
		}
	);

	return @available_cpus if wantarray;
	return \@available_cpus;
}

sub job_locality_factor {
	my ($self, $assigned_processors) = @_;

	return $self->job_used_clusters($assigned_processors) / ceil($assigned_processors->size() / $self->cluster_size());
}

1;
