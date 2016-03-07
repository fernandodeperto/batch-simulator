package Platform;
use strict;
use warnings;

use Log::Log4perl qw(get_logger);
use Data::Dumper;
use List::Util qw(min max sum);
use POSIX;
use XML::Smart;
use Carp;

use Tree;

use lib 'ProcessorRange/blib/lib', 'ProcessorRange/blib/arch';
use ProcessorRange;

# Default power, latency and bandwidth values
use constant CLUSTER_POWER => "23.492E9";
use constant CLUSTER_BANDWIDTH => "1.25E9";
use constant LINK_BANDWIDTH => "1.25E9";

# Constructors and helper functions

sub new {
	my $class = shift;
	my $levels = shift;

	my $self = {
		levels => $levels,
	};

	bless $self, $class;
	return $self;
}

sub processors_number {
	my $self = shift;
	return $self->{levels}->[-1];
}

sub cluster_size {
	my $self = shift;

	my $last_level = $#{$self->{levels}};
	return $self->{levels}->[$last_level]/$self->{levels}->[$last_level - 1];
}

# Tree structure

sub build_tree {
	my $self = shift;
	my $available_cpus = shift;

	$self->{root} = $self->_build_tree(0, 0, $available_cpus);
	return;
}

sub _build_tree {
	my $self = shift;
	my $level = shift;
	my $node = shift;
	my $available_cpus = shift;

	my $next_level_nodes = $self->{levels}->[$level + 1]/$self->{levels}->[$level];
	my @next_level_nodes_ids = map {$next_level_nodes * $node + $_} (0..($next_level_nodes - 1));

	# Last level before the leafs/nodes
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

sub choose_combination {
	my $self = shift;
	my $requested_cpus = shift;

	$self->_score($self->{root}, 0, $requested_cpus);
	return $self->_choose_combination($self->{root}, 0, $requested_cpus);
}

sub _choose_combination {
	my $self = shift;
	my $tree = shift;
	my $level = shift;
	my $requested_cpus = shift;

	# Return nothing if requested_cpus is 0
	return unless ($requested_cpus);

	# Return if at the last level
	return [$tree->content()->{id}, $requested_cpus] if ($level == $#{$self->{levels}} - 1);

	my $best_combination = $tree->content()->{$requested_cpus}->{combination};

	my @children = @{$tree->children()};
	return map {$self->_choose_combination($_, $level + 1, shift @{$best_combination})} (@children);
}

sub choose_cpus {
	my $self = shift;
	my $requested_cpus = shift;

	$self->_score($self->{root}, 0, $requested_cpus);
	return $self->_choose_cpus($self->{root}, $requested_cpus);
}

sub _choose_cpus {
	my $self = shift;
	my $tree = shift;
	my $requested_cpus = shift;

	# No requested cpus
	return unless $requested_cpus;

	my @children = @{$tree->children()};

	# Leaf node/CPU
	return $tree->content()->{id} if (defined $tree->content()->{id});

	my $best_combination = $tree->content()->{$requested_cpus};

	my @combination_parts = split('-', $best_combination->{combination});

	return map {$self->_choose_cpus($_, shift @combination_parts)} (@children);
}

sub _combinations {
	my $self = shift;
	my $tree = shift;
	my $requested_cpus = shift;
	my $node = shift;

	my @children = @{$tree->children()};
	my $last_child = $#children;

	# Last node
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
	my $self = shift;
	my $tree = shift;
	my $level = shift;
	my $requested_cpus = shift;

	# No needed CPUs
	return 0 unless $requested_cpus;

	my $max_depth = $#{$self->{levels}} - 1;

	# Leaf/CPU
	return 0 if ($level == $max_depth);

	# Best combination already saved
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

		# Add to the score if there is communication between different child nodes
		$score += ($max_depth + 1 - $level) if (max(@{$combination}) < $requested_cpus);

		if ($score < $best_combination{score}) {
			$best_combination{score} = $score;
			$best_combination{combination} = $combination;
		}
	}

	$tree->content()->{$requested_cpus} = \%best_combination;
	return $best_combination{score};
}

sub generate_all_combinations {
	my $self = shift;
	my $requested_cpus = shift;

	return $self->_combinations($self->{root}, $requested_cpus, 0);
}

sub _score_function_pnorm {
	my $self = shift;
	my $child_requested_cpus = shift;
	my $requested_cpus = shift;
	my $level = shift;

	my $max_depth = scalar @{$self->{levels}} - 1;

	return $child_requested_cpus * ($requested_cpus - $child_requested_cpus) * pow(($max_depth - $level) * 2, $self->{norm});
}

# Linear structure

sub build_structure {
	my $self = shift;
	my $available_cpus = shift;

	my $last_level = $#{$self->{levels}} - 1;

	my @cpus_structure;

	for my $level (0..$last_level) {
		$cpus_structure[$level] = [];

		my $nodes_per_block = $self->{levels}->[$last_level]/$self->{levels}->[$last_level - $level];

		for my $block (0..($self->{levels}->[$last_level - $level] - 1)) {
			my $block_content = {
				total_size => 0,
				total_original_size => $self->{levels}->[$#{$self->{levels}}]/$self->{levels}->[$last_level - $level],
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

	return \@cpus_structure;
}

# Speedup generation

sub node_level_distance {
	my $self = shift;
	my $first_node = shift;
	my $second_node = shift;

	my $last_level = $#{$self->{levels}};

	for my $level (0..($last_level - 1)) {
		my $cpus_group = $self->{levels}->[$last_level]/$self->{levels}->[$level + 1];
		return $last_level - $level if (int $first_node/$cpus_group != int $second_node/$cpus_group);
	}

	return 0;
}

sub generate_speedup {
	my $self = shift;
	my $benchmark = shift;
	my $platform_file = shift;

	my $smpi_script = './scripts/smpi/smpireplay.sh';
	my $hosts_file = '/tmp/hosts';

	unless (defined $platform_file) {
		$platform_file = '/tmp/platform';
		$self->build_platform_xml();
		$self->save_platform_xml($platform_file);
	}

	my $last_level = $#{$self->{levels}};
	my @hosts_configs = reverse map {[0, int($self->{levels}->[-1]/$self->{levels}->[$_])]} (1..$last_level);
	my $cpus_number = 2;

	my @results;

	for my $hosts_config (@hosts_configs) {
		save_hosts_file($hosts_config, $hosts_file);

		my $result = `$smpi_script $cpus_number $platform_file $hosts_file $benchmark 2>&1`;

		unless ($result =~ /Simulation time (\d*\.\d*)/) {
			print STDERR "$smpi_script $cpus_number $platform_file $hosts_file $benchmark\n";
			print STDERR "$result\n";
			die 'error running benchmark';
		}

		my $distance = $self->node_level_distance($hosts_config->[0], $hosts_config->[1]);
		push @results, $1;
	}

	my $base_runtime = $results[0];
	@results = map {$_/$base_runtime} (@results);
	print Dumper(@results);
	@{$self->{speedup}} = @results;

	return;
}

sub set_speedup {
	my $self = shift;
	my $latencies = shift;

	@{$self->{speedup}} = reverse map {$_/$latencies->[-1]} (@{$latencies});
	return;
}

sub save_hosts_file {
	my $hosts_config = shift;
	my $hosts_file = shift;

	open(my $file, '>', $hosts_file);
	print $file join("\n", @{$hosts_config}) . "\n";
	close($file);
}

sub speedup {
	my $self = shift;
	my $level = shift;

	return unless (defined $self->{speedup});
	return $self->{speedup}->[$level - 1];
}

# Platform XML

sub build_platform_xml {
	my $self = shift;

	my @platform_parts = @{$self->{levels}};
	my $xml = XML::Smart->new();

	$xml->{platform} = {version => 3};

	# Root system
	$xml->{platform}{AS} = {
		id => "AS_Root",
		routing => "Floyd",
	};

	# Tree system
	$xml->{platform}{AS}{AS} = {
		id => "AS_Tree",
		routing => "Floyd",
	};

	# Push the first router
	push @{$xml->{platform}{AS}{AS}{router}}, {id => "R-0-0"};

	# Build levels
	for my $level (1..($#platform_parts - 1)) {
		my $nodes_number = $platform_parts[$level];

		for my $node_number (0..($nodes_number - 1)) {
			push @{$xml->{platform}{AS}{AS}{router}}, {id => "R-$level-$node_number"};

			my $father_node = int $node_number/($platform_parts[$level]/$platform_parts[$level - 1]);
			push @{$xml->{platform}{AS}{AS}{link}}, {
				id => "L-$level-$node_number",
				bandwidth => LINK_BANDWIDTH,
				latency => $self->{latencies}->[$level -1],
			};

			push @{$xml->{platform}{AS}{AS}{route}}, {
				src => 'R-' . ($level - 1) . "-$father_node",
				dst => "R-$level-$node_number",
				link_ctn => {id => "L-$level-$node_number"},
			};
		}
	}

	# Master host
	push @{$xml->{platform}{AS}{cluster}}, {
			id => 'C-MH',
			prefix => 'master_host',
			suffix => '',
			radical => '0-0',
			power => CLUSTER_POWER,
			bw => CLUSTER_BANDWIDTH,
			lat => $self->{latencies}->[$#{$self->{latencies}}],
			router_id => 'R-MH',
	};

	push @{$xml->{platform}{AS}{link}}, {
		id => 'L-MH',
		bandwidth => LINK_BANDWIDTH,
		latency => $self->{latencies}->[$#{$self->{latencies}}],
	};

	push @{$xml->{platform}{AS}{ASroute}}, {
		src => 'C-MH',
		gw_src => 'R-MH',
		dst => 'AS_Tree',
		gw_dst => 'R-0-0',
		link_ctn => {id => 'L-MH'},
	};

	# Clusters
	for my $cluster (0..($platform_parts[$#platform_parts - 1] - 1)) {
		push @{$xml->{platform}{AS}{cluster}}, {
			id => "C-$cluster",
			prefix => "",
			suffix => "",
			radical => ($cluster * $self->cluster_size()) . '-' . (($cluster + 1) * $self->cluster_size() - 1),
			power => CLUSTER_POWER,
			bw => CLUSTER_BANDWIDTH,
			lat => $self->{latencies}->[$#{$self->{latencies}}],
			router_id => "R-$cluster",
		};

		push @{$xml->{platform}{AS}{link}}, {
			id => "L-$cluster",
			bandwidth => LINK_BANDWIDTH,
			latency => $self->{latencies}->[$#{$self->{latencies}}],
		};

		push @{$xml->{platform}{AS}{ASroute}}, {
			src => "C-$cluster",
			gw_src => "R-$cluster",
			dst => "AS_Tree",
			gw_dst => 'R-' . ($#platform_parts - 1) . "-$cluster",
			link_ctn => {id => "L-$cluster"},
		}
	}

	$self->{xml} = $xml;
	return;
}

sub save_platform_xml {
	my $self = shift;
	my $filename = shift;

	open(my $file, '>', $filename);

	print $file "<?xml version=\'1.0\'?>\n" . "<!DOCTYPE platform SYSTEM \"http://simgrid.gforge.inria.fr/simgrid.dtd\">\n" . $self->{xml}->data(noheader => 1, nometagen => 1);

	return;
}

# Platform level measure for jobs

sub job_level_distance {
	my $self = shift;
	my $assigned_processors = shift;

	my @used_clusters = $self->job_used_clusters($assigned_processors);
	my $last_level = $#{$self->{levels}};
	my $clusters_number = $self->{levels}->[$last_level - 1];

	# Return 1 if there is only one cluster
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

sub job_relative_level_distance {
	my $self = shift;
	my $used_clusters = shift;
	my $requested_cpus = shift;

	my $last_level = $#{$self->{levels}};
	my $clusters_number = $self->{levels}->[$last_level - 1];

	# Return 1 if there is only one cluster
	return 1 if (scalar @{$used_clusters} == 1);

	# Compute minimum level distance for the job
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
		my $clusters_side = int($used_clusters->[0] / $clusters_per_side);

		for my $cluster_id (1..$#{$used_clusters}) {
			if (int($used_clusters->[$cluster_id] / $clusters_per_side) != $clusters_side) {
				return ($last_level - $level) / $minimum_level_distance;
			}
		}
	}

	return 1;
}

# Contiguity and locality

# Returns 1 if the assigned processors form a contiguous block and 0 if not.
# Receives as parameter a list of processor ranges in the form of a
# ProcessorRange object. Evaluation is in scalar context.
sub job_contiguity {
	my $self = shift;
	my $assigned_processors = shift;

	my @ranges = $assigned_processors->pairs();

	return 1 if (@ranges == 1);
	return 1 if (@ranges == 2 and $ranges[0]->[0] == 0
	and $ranges[1]->[1] == $self->processors_number()- 1);

	return 0;
}

# Returns the contiguity factor for a job. Receives as parameter a list of
# processor ranges in the form of a ProcessorRange object. Evaluation may be in
# list or schalar context, depending on how the retun value will be used.
sub job_contiguity_factor {
	my $self = shift;
	my $assigned_processors = shift;

	my $ranges = $assigned_processors->pairs();

	return 1 if (@{$ranges} == 2 and $ranges->[0]->[0] == 0
	and $ranges->[1]->[1] == $self->processors_number()- 1);

	return @{$ranges};
}

# Returns the locality factor for a job. Receives as parameter a list of
# processor ranges in the form of a ProcessorRange object. Evaluation is done
# in scalar context.
sub job_locality {
	my $self = shift;
	my $assigned_processors = shift;

	my @used_clusters = $self->job_used_clusters($assigned_processors);

	return 1 if (@used_clusters == ceil($assigned_processors->size() / $self->cluster_size()));
	return 0;
}

# Returns the list of used clusters by a job. Receives as parameter a list of
# processor ranges in the form of a ProcessorRange object. Evaluation may be in
# list or scalar context, depending on how the return value will be used.
sub job_used_clusters {
	my $self = shift;
	my $assigned_processors = shift;

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

# Returns the locality factor for a job. Receives as parameter a list of
# processor ranges in the form of a ProcessorRange object. Evaluation is done
# in scalar context.
sub job_locality_factor {
	my $self = shift;
	my $assigned_processors = shift;

	return $self->job_used_clusters($assigned_processors)
	/ ceil($assigned_processors->size() / $self->cluster_size());
}

1;
