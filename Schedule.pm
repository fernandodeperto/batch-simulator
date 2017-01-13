package Schedule;
use parent 'Displayable';

use strict;
use warnings;

use List::Util qw(max sum);
use Time::HiRes qw(time);
use Data::Dumper;
use POSIX qw(pow);

use Util qw($config);
use Platform;
use Job;

# Constructors

sub new {
	my ($class, $platform, $trace) = @_;

	my $self = {
		trace => $trace,
		platform => $platform,
		cmax => 0,
	};

	die "not enough processors: ", $self->{trace}->needed_cpus() if $self->{trace}->needed_cpus() > $self->{platform}->processors_number();
	$self->{trace}->unassign_jobs(); # make sure the trace is clean

	bless $self, $class;
	return $self;
}

sub run {
	my ($self) = @_;

	$self->assign_job($_) for @{$self->{trace}->jobs()};

	return;
}

# Getters and setters

sub trace {
	my ($self) = @_;
	return $self->{trace};
}

sub run_time {
	my ($self) = @_;
	return $self->{run_time};
}

# Metrics

sub cmax {
	my ($self) = @_;
	return max map {$_->real_ending_time()} (@{$self->{trace}->jobs()});
}

sub sum_flow_time {
	my ($self) = @_;
	return sum map {$_->flow_time()} @{$self->{trace}->jobs()};
}

sub max_flow_time {
	my ($self) = @_;
	return max map {$_->flow_time()} @{$self->{trace}->jobs()};
}

sub mean_flow_time {
	my ($self) = @_;
	return $self->sum_flow_time() / @{$self->{trace}->jobs()};
}

sub sum_stretch {
	my ($self) = @_;
	return sum map {$_->bounded_stretch()} @{$self->{trace}->jobs()};
}

sub max_stretch {
	my ($self) = @_;
	return max map {$_->bounded_stretch()} @{$self->{trace}->jobs()};
}

sub mean_stretch {
	my ($self) = @_;
	return (sum map {$_->stretch()} @{$self->{trace}->jobs()}) / @{$self->{trace}->jobs()};
}

sub bounded_stretch {
	my ($self, $bound) = @_;

	$bound = 10 unless defined $bound;

	my $jobs_number = scalar @{$self->{trace}->jobs()};
	my $total_bounded_stretch = sum map {$_->bounded_stretch($bound)} (@{$self->{trace}->jobs()});

	return $total_bounded_stretch/$jobs_number;
}

sub stretch_sum_of_squares {
	my ($self) = @_;
	return sqrt(sum map {$_->bounded_stretch(10) ** 2} (@{$self->{trace}->jobs()}));
}

sub stretch_with_cpus_squared {
	my ($self) = @_;
	return sum map {$_->bounded_stretch_with_cpus_squared(10)} (@{$self->{trace}->jobs()});
}

sub stretch_with_cpus_log {
	my ($self) = @_;
	return sum map {$_->bounded_stretch_with_cpus_log(10)} (@{$self->{trace}->jobs()});
}

sub flow_time_pnorm {
	my ($self) = @_;

	my $pnorm = $config->param('parameters.pnorm');

	return pow((sum map {defined $_->flow_time() ? pow($_->flow_time(), $pnorm) : 0} @{$self->{trace}->jobs()}), 1/$pnorm);
}

sub stretch_pnorm {
	my ($self) = @_;

	my $pnorm = $config->param('parameters.pnorm');

	return pow((sum map {defined $_->bounded_stretch() ? pow($_->bounded_stretch(), $pnorm) : 0} @{$self->{trace}->jobs()}), 1/$pnorm);
}

sub contiguous_jobs_number {
	my ($self) = @_;
	return sum map {$self->{platform}->job_contiguity($_->assigned_processors())} (@{$self->{trace}->jobs()});
}

sub contiguity_factor {
	my ($self) = @_;

	my $total_contiguity_factor = sum map {$self->{platform}->job_contiguity_factor($_->assigned_processors())} (@{$self->{trace}->jobs()});

	return $total_contiguity_factor/scalar @{$self->{trace}->jobs()};
}

sub local_jobs_number {
	my ($self) = @_;
	return sum map {$self->{platform}->job_locality($_->assigned_processors())} (@{$self->{trace}->jobs()});
}

sub locality_factor {
	my ($self) = @_;

	my $total_locality_factor = sum map {$self->{platform}->job_locality_factor($_->assigned_processors())} (@{$self->{trace}->jobs()});

	return $total_locality_factor/scalar @{$self->{trace}->jobs()};
}

sub platform_level_factor {
	my ($self) = @_;

	my $job_level_distances = sum map {$self->{platform}->job_relative_level_distance($_->assigned_processors(), $_->requested_cpus())} (@{$self->{trace}->jobs()});

	return $job_level_distances / scalar @{$self->{trace}->jobs()};
}

sub job_success_rate {
	my ($self) = @_;
	return sum map {($_->status() == JOB_STATUS_COMPLETED) ? 1 : 0} (@{$self->{trace}->jobs()});
}

# SVG

sub save_svg {
	my ($self, $svg_filename) = @_;
	my $time = $self->{current_time};
	$time = 0 unless defined $time;

	open(my $filehandle, '>', "$svg_filename") or die "unable to open $svg_filename";

	my $cmax = $self->cmax();
	$cmax = 1 unless defined $cmax;
	print $filehandle "<svg width=\"800\" height=\"600\">\n";
	my $w_ratio = 800/$cmax;
	my $h_ratio = 600/$self->{platform}->processors_number();

	my $current_x = $w_ratio * $time;
	print $filehandle "<line x1=\"$current_x\" x2=\"$current_x\" y1=\"0\" y2=\"600\" style=\"stroke:rgb(255,0,0);stroke-width:5\"/>\n";

	my $clusters_number = POSIX::ceil($self->{platform}->processors_number()
		/ $self->{platform}->cluster_size());
	my $cluster_size = 600/$self->{platform}->processors_number()
		* $self->{platform}->cluster_size();
#	for my $cluster (1..$clusters_number) {
#		my $cluster_y = $cluster * $cluster_size;
#		print $filehandle "<line x1=\"0\" x2=\"800\" y1=\"$cluster_y\" y2=\"$cluster_y\" style=\"stroke:rgb(255,0,0);stroke-width:1\"/>\n";
#	}

	$_->svg($filehandle, $w_ratio, $h_ratio, $time, $self->{platform}) for grep {defined $_->starting_time()} (@{$self->{trace}->jobs()});

	print $filehandle "</svg>\n";
	close $filehandle;
	return;
}

1;

