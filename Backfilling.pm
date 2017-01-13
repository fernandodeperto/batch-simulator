package Backfilling;
use parent 'Schedule';
use strict;
use warnings;

use Exporter qw(import);
use Time::HiRes qw(time);
use Data::Dumper;
use List::Util qw(max min shuffle);
use Math::Random qw(random_normal);

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

sub new {
	my $class = shift;

	# Additional parameters
	my $reduction_algorithm = shift;
	my $communication_level = shift;

	my $self = $class->SUPER::new(@_);

	$self->{execution_profile} = ExecutionProfile->new(
		$self->{platform}->processors_number(),
		$reduction_algorithm,
	);

	$self->{current_time} = 0;
	$self->{communication_level} = $communication_level;

	return $self;
}

sub run {
	my $self = shift;

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
	my $self = shift;
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
	my $self = shift;
	my $latest_ending_time = shift;

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
	my $self = shift;
	my $job = shift;

	my ($starting_time, $chosen_processors) = $self->{execution_profile}->find_first_profile($job);

	# here we can decide the new run time based on the platform level
	if (defined $self->{platform}->slowdown()) {
		my $job_platform_level = $self->{platform}->job_level_distance($chosen_processors);
		my $job_minimum_level = $self->{platform}->job_minimum_level_distance($job->requested_cpus());

		if ($job_platform_level != $job_minimum_level) {
			my $slowdown = $self->{platform}->slowdown($job_platform_level - 1) /
				$self->{platform}->slowdown($job_minimum_level - 1);

			my $new_job_run_time = int((1 - $self->{communication_level}) * $job->run_time() +
				$slowdown * $self->{communication_level} * $job->run_time());
				

			if ($new_job_run_time > $job->requested_time()) {
				$job->run_time($job->requested_time());
				$job->status(JOB_STATUS_FAILED);
			} else {
				$job->run_time($new_job_run_time);
			}
		}
	}

	$job->assign($starting_time, $chosen_processors);
	$self->{execution_profile}->add_job($starting_time, $job);

	return;
}

1;
