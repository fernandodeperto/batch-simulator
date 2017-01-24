package Backfilling;
use strict;
use warnings;

use Exporter qw(import);
use Time::HiRes qw(time);
use Data::Dumper;
use List::Util qw(max min shuffle sum);
use Math::Random qw(random_normal random_uniform);
use Switch;

use Util qw($config);
use ExecutionProfile;
use Heap;
use Event;
use Platform;
use Job;

use constant {
	SUBMISSION_EVENT => 0,
	JOB_START_EVENT => 1,
	JOB_COMPLETION_EVENT => 2,
};

# Constructors

sub new {
	my (
		$class,
		$reduction_algorithm,
		$benchmark_data,
		$comm_data,
		$platform,
		$trace,
	) = @_;

	my $self = {
		benchmark_data => $benchmark_data,
		cmax => 0,
		comm_data => $comm_data,
		current_time => 0,
		execution_profile => ExecutionProfile->new(
			$platform->processors_number(),
			$reduction_algorithm,
		),
		platform => $platform,
		trace => $trace,
	};

	die "not enough processors: ", $self->{trace}->needed_cpus() if
		$self->{trace}->needed_cpus() > $self->{platform}->processors_number();


	bless $self, $class;
	return $self;
}

# Getters and setters

sub trace {
	my ($self) = @_;
	return $self->{trace};
}

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

# Functions

sub run {
	my ($self) = @_;

	$self->{reserved_jobs} = []; # jobs not started yet
	$self->{started_jobs} = {}; # jobs that have already started

	$self->{events} = Heap->new(Event->new(-1, -1));

	$self->{events}->add(
		Event->new(
			SUBMISSION_EVENT,
			$_->submit_time(),
			$_
		)
	) for (@{$self->{trace}->jobs()});

	$self->{run_time} = time();

	while (my @events = $self->{events}->retrieve_all()) {
		$self->{current_time} = $events[0]->timestamp();
		$self->{execution_profile}->set_current_time($self->{current_time});

		my @typed_events;
		push @{$typed_events[$_->type()]}, $_ for @events;

		for my $event (@{$typed_events[JOB_COMPLETION_EVENT]}) {
			my $job = $event->payload();

			$self->{execution_profile}->remove_job($job, $self->{current_time}) unless $job->requested_time() == $job->run_time();
		}

		# reassign all reserved jobs if any job finished
		$self->reassign_jobs() if $config->param('backfilling.reassign_jobs') and scalar @{$typed_events[JOB_COMPLETION_EVENT]};

		# submission events
		@{$typed_events[SUBMISSION_EVENT]} = sort {$a->payload()->requested_time() <=> $b->payload()->requested_time()} (@{$typed_events[SUBMISSION_EVENT]}) if $config->param('backfilling.sort_sumitted_jobs');
		@{$typed_events[SUBMISSION_EVENT]} = shuffle @{$typed_events[SUBMISSION_EVENT]} if $config->param('backfilling.shuffle_submitted_jobs');

		for my $event (@{$typed_events[SUBMISSION_EVENT]}) {
			my $job = $event->payload();

			$self->assign_job($job);
			die "job " . $job->job_number() . " was not assigned" unless defined $job->starting_time();

			push @{$self->{reserved_jobs}}, $job;

			$self->{events}->add(
				Event->new(
					JOB_START_EVENT,
					$job->starting_time(),
					$job
				)
			);

		}

		$self->start_jobs();
	}

	# all jobs should be scheduled and started
	die 'there are still jobs in the reserved queue' if @{$self->{reserved_jobs}};

	$self->{execution_profile}->free_profiles();

	$self->{run_time} = time() - $self->{run_time};

	return;
}

sub start_jobs {
	my ($self) = @_;
	my @remaining_reserved_jobs;

	@{$self->{reserved_jobs}} = sort {$a->requested_time() <=> $b->requested_time()} (@{$self->{reserved_jobs}}) if $config->param('backfilling.sort_reserved_jobs');

	for my $job (@{$self->{reserved_jobs}}) {
		if ($job->starting_time() == $self->{current_time}) {
			$self->{events}->add(
				Event->new(
					JOB_COMPLETION_EVENT,
					$job->real_ending_time(),
					$job
				)
			);
		} else {
			push @remaining_reserved_jobs, $job;
		}
	}

	$self->{reserved_jobs} = \@remaining_reserved_jobs;
	return;
}

sub reassign_jobs {
	my ($self, $latest_ending_time) = @_;

	for my $job (@{$self->{reserved_jobs}}) {
		next unless $self->{execution_profile}->available_processors($self->{current_time}) >= $job->requested_cpus();

		my $job_starting_time = $job->starting_time();
		my $assigned_processors = $job->assigned_processors();

		$self->{execution_profile}->remove_job($job, $self->{current_time});

		unless ($self->{execution_profile}->could_start_job($job, $self->{current_time})) {
			$self->{execution_profile}->add_job($job_starting_time, $job);
			next;
		}

		my $new_processors = $self->{execution_profile}->get_free_processors($job, $self->{current_time});

		unless (defined $new_processors) {
			$self->{execution_profile}->add_job($job_starting_time, $job);
			next;
		}

		$job->assign($self->{current_time}, $new_processors);
		$self->{execution_profile}->remove_job($job, $self->{current_time});
		$self->{execution_profile}->add_job($self->{current_time}, $job);
	}

	return;
}

sub assign_job {
	my ($self, $job) = @_;

	my ($starting_time, $chosen_processors) = $self->{execution_profile}->find_first_profile($job);

	my $job_platform_level = $self->{platform}->job_level_distance($chosen_processors);
	my $job_minimum_level = $self->{platform}->job_minimum_level_distance($job->requested_cpus());

	if ($job_platform_level != $job_minimum_level) {
		my $communication_level;
		my $chosen_benchmark;

		switch ($config->param('backfilling.penalty_job_assignment')) {
			case 'random_benchmark' {
				$chosen_benchmark = $self->{comm_data}->{$job->job_number()}->{BENCHMARK};
				$communication_level = $self->{benchmark_data}->{$chosen_benchmark}->{COMMUNICATION_TIME};
			}

			case 'random_percentage_normal' {
				$communication_level = $self->{comm_data}->{$job->job_number()}->{NORMAL_PERCENT};
			}

			case 'random_percentage_uniform' {
				$communication_level = $self->{comm_data}->{$job->job_number()}->{UNIFORM_PERCENT};
			}

			else {
				die 'unknown communication profile used';
			}
		}

		my $penalty_rate;
		switch ($config->param('backfilling.penalty_function')) {
			case 'linear' {
				$penalty_rate = ($job_platform_level - $job_minimum_level) * $config->param('backfilling.penalty_function_linear_factor');
			}

			case 'quadratic' {
				$penalty_rate = $config->param('backfilling.penalty_function_quadratic_factor') ** ($job_platform_level - $job_minimum_level)
			}

			case 'benchmark' {
				die unless $config->param('backfilling.penalty_job_assignment') eq 'random_benchmark';
			}

			else {
				die 'unknown penalty function';
			}
		}

		my $new_job_run_time = int((1 - $communication_level) * $job->run_time() +
			$penalty_rate * $communication_level * $job->run_time());

		if ($new_job_run_time > $job->requested_time()) {
			$job->run_time($job->requested_time());
			$job->status(JOB_STATUS_FAILED);
		} else {
			$job->run_time($new_job_run_time);
		}
	}

	$job->assign($starting_time, $chosen_processors);
	$self->{execution_profile}->add_job($starting_time, $job);

	return;
}

1;
