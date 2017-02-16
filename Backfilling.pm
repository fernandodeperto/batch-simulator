package Backfilling;
use strict;
use warnings;

use Exporter qw(import);
use Time::HiRes qw(time);
use Data::Dumper;
use List::Util qw(max min shuffle sum);
use Switch;
use POSIX qw(pow);
use Log::Log4perl qw(:no_extra_logdie_message get_logger);

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
		$platform,
		$trace,
		$penalty_function,
		$penalty_factor,
	) = @_;

	my $self = {
		cmax => 0,
		current_time => 0,
		execution_profile => ExecutionProfile->new(
			$platform->processors_number(),
			$reduction_algorithm,
		),
		platform => $platform,
		trace => $trace,
		penalty_function => $penalty_function,
		penalty_factor => $penalty_factor,
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

sub run_time {
	my ($self) = @_;
	return $self->{run_time};
}

# Functions

sub run {
	my ($self) = @_;

	my $logger = get_logger('Backfilling.run');

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

		$logger->trace("current time: $self->{current_time}");

		my @typed_events;
		push @{$typed_events[$_->type()]}, $_ for @events;

		$logger->trace("submission events: @{$typed_events[SUBMISSION_EVENT]}") if $typed_events[SUBMISSION_EVENT];
		$logger->trace("ending events: @{$typed_events[JOB_COMPLETION_EVENT]}") if $typed_events[JOB_COMPLETION_EVENT];

		for my $event (@{$typed_events[JOB_COMPLETION_EVENT]}) {
			my $job = $event->payload();
			$self->{execution_profile}->remove_job($job, $self->{current_time}) unless $job->requested_time() == $job->run_time();
		}

		# reassign all reserved jobs if any job finished
		$self->reassign_jobs() if $config->param('backfilling.reassign_jobs') and scalar @{$typed_events[JOB_COMPLETION_EVENT]};

		for my $event (@{$typed_events[SUBMISSION_EVENT]}) {
			my $job = $event->payload();

			$self->assign_job($job);
			$logger->logdie("job " . $job->job_number() . " was not assigned") unless defined $job->starting_time();

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
	$logger->logdie('there are still jobs in the reserved queue') if @{$self->{reserved_jobs}};

	$self->{execution_profile}->free_profiles();
	$self->{run_time} = time() - $self->{run_time};
	return;
}

sub start_jobs {
	my ($self) = @_;
	my @remaining_reserved_jobs;

	my $logger = get_logger('Backfilling::start_jobs');

	for my $job (@{$self->{reserved_jobs}}) {
		if ($job->starting_time() == $self->{current_time}) {
			$logger->trace('job ' . $job->job_number() . ' starting');

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
	my ($self) = @_;

	my $logger = get_logger('Backfilling:reassign_jobs');

	for my $job (@{$self->{reserved_jobs}}) {
		$logger->trace('trying to reassign job ' . $job->job_number());

		next unless $self->{execution_profile}->available_processors($self->{current_time}) >= $job->requested_cpus();

		$logger->trace('available processors');

		my $job_starting_time = $job->starting_time();
		my $assigned_processors = $job->assigned_processors();

		$self->{execution_profile}->remove_job($job, $self->{current_time});

		unless ($self->{execution_profile}->could_start_job($job, $self->{current_time})) {
			$self->{execution_profile}->add_job($job_starting_time, $job);
			next;
		}

		$logger->trace('could start job');

		my $new_processors = $self->{execution_profile}->get_free_processors($job, $self->{current_time});

		unless (defined $new_processors) {
			$self->{execution_profile}->add_job($job_starting_time, $job);
			next;
		}

		$logger->trace('new processors defined');

		$job->assign($self->{current_time}, $new_processors);
		$self->{execution_profile}->add_job($self->{current_time}, $job);
	}

	return;
}

sub assign_job {
	my ($self, $job) = @_;

	my $logger = get_logger('Backfilling::assign_job');

	$logger->trace('assigning job ' . $job->job_number());

	my ($starting_time, $chosen_processors) = $self->{execution_profile}->find_first_profile($job);

	$logger->trace("chose starting time:processors $starting_time:$chosen_processors");

	my $job_platform_level = $self->{platform}->job_level_distance($chosen_processors);
	my $job_minimum_level = $self->{platform}->job_minimum_level_distance($job->requested_cpus());

	$logger->trace("job levels $job_platform_level/$job_minimum_level");

	if ($job_platform_level != $job_minimum_level) {
		my $new_job_run_time;

		switch ($self->{penalty_function}) {
			case 'linear' {
				$new_job_run_time = $job->run_time() * (1 + ($job_platform_level - $job_minimum_level) * $self->{penalty_factor});
			}

			case 'quadratic' {
				$new_job_run_time = $job->run_time() * $self->{penalty_factor} ** ($job_platform_level - $job_minimum_level)
			}
		}

		$job->original_run_time($job->run_time());

		$logger->trace("using penalty function $self->{penalty_function} and factor $self->{penalty_factor}");
		$logger->trace("new job run time: $new_job_run_time (from " . $job->run_time() . ')');

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
