package Backfilling;
use parent 'Schedule';
use strict;
use warnings;

use Exporter qw(import);
use Time::HiRes qw(time);
use Data::Dumper;
use List::Util qw(min);

use ExecutionProfile;
use Heap;
use Event;
use Platform;
use Job;

use Debug;

use constant {
	JOB_COMPLETED_EVENT => 0,
	SUBMISSION_EVENT => 1
};

sub new {
	my $class = shift;
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

	$self->{events} = Heap->new(Event->new(SUBMISSION_EVENT, -1));

	$self->{events}->add(
		Event->new(
			SUBMISSION_EVENT,
			$_->submit_time(),
			$_
		)
	) for (@{$self->{trace}->jobs()});

	$self->{run_time} = time();

	while (my @events = $self->{events}->retrieve_all()) {
		# events coming from the heap will have the same time and type
		my $events_timestamp = $events[0]->timestamp();
		$self->{current_time} = $events_timestamp;

		$self->{execution_profile}->set_current_time($self->{current_time});

		my @typed_events;
		push @{$typed_events[$_->type()]}, $_ for @events; # 2 lists, one for each event type

		##DEBUG_BEGIN
		print STDERR "[$self->{current_time}] scheduled jobs:\n", join("\n", @{$self->{reserved_jobs}}), "\n";
		##DEBUG_END

		# ending event
		for my $event (@{$typed_events[JOB_COMPLETED_EVENT]}) {

			my $job = $event->payload();

			##DEBUG_BEGIN
			print STDERR "[$self->{current_time}] job " . $job->job_number() . " ending\n";
			##DEBUG_END

			delete $self->{started_jobs}->{$job->job_number()};

			##DEBUG_BEGIN
			print STDERR "[$self->{current_time}] before removing:\n", $self->{execution_profile}, "\n";
			##DEBUG_END

			$self->{execution_profile}->remove_job($job, $self->{current_time}) unless $job->requested_time() == $job->run_time();

			##DEBUG_BEGIN
			print STDERR "[$self->{current_time}] after removing:\n", $self->{execution_profile}, "\n";
			##DEBUG_END
		}

		# reassign all reserved jobs if any job finished
		$self->reassign_jobs() if (@{$typed_events[JOB_COMPLETED_EVENT]});

		# submission events
		for my $event (@{$typed_events[SUBMISSION_EVENT]}) {
			my $job = $event->payload();


			$self->assign_job($job);
			die "job " . $job->job_number() . " was not assigned" unless defined $job->starting_time();
			push @{$self->{reserved_jobs}}, $job;
		}

		$self->start_jobs();
	}

	# all jobs should be scheduled and started
	die 'there are still jobs in the reserved queue: ' . join("\n", @{$self->{reserved_jobs}}) if @{$self->{reserved_jobs}};

	$self->{execution_profile}->free_profiles();

	# time measure
	$self->{run_time} = time() - $self->{run_time};

	return;
}

sub start_jobs {
	my $self = shift;
	my @remaining_reserved_jobs;
	my @newly_started_jobs;

	for my $job (@{$self->{reserved_jobs}}) {
		if ($job->starting_time() == $self->{current_time}) {
			##DEBUG_BEGIN
			print STDERR "[$self->{current_time}] job " . $job->job_number() . " starting\n";
			##DEBUG_END

			$self->{events}->add(
				Event->new(
					JOB_COMPLETED_EVENT,
					$job->real_ending_time(),
					$job
				)
			);

			$self->{started_jobs}->{$job->job_number()} = $job;
			push @newly_started_jobs, $job;
		} else {
			push @remaining_reserved_jobs, $job;
		}
	}

	$self->{reserved_jobs} = \@remaining_reserved_jobs;
	return;
}

sub reassign_jobs {
	my $self = shift;

	for my $job (@{$self->{reserved_jobs}}) {
		##DEBUG_BEGIN
		print STDERR "trying reassign job ", $job->job_number(), "\n";
		##DEBUG_END

		##DEBUG_BEGIN
		print STDERR "execution profile:\n", $self->{execution_profile}, "\n";
		print STDERR "available cpus: ", scalar $self->{execution_profile}->available_processors($self->{current_time}), "\n";
		##DEBUG_END

		if ($self->{execution_profile}->available_processors($self->{current_time}) >= $job->requested_cpus()) {
			my $job_starting_time = $job->starting_time();
			my $assigned_processors = $job->assigned_processors();

			##DEBUG_BEGIN
			print STDERR "enough processors for job " . $job->job_number() . "\n";
			##DEBUG_END

			$self->{execution_profile}->remove_job($job, $self->{current_time});

			my $new_processors;
			if ($self->{execution_profile}->could_start_job($job, $self->{current_time})) {
				##DEBUG_BEGIN
				print STDERR "could start job " . $job->job_number() . "\n";
				##DEBUG_END

				$new_processors = $self->{execution_profile}->get_free_processors($job, $self->{current_time});
			}

			if (defined $new_processors) {
				##DEBUG_BEGIN
				print STDERR "reassigning job " . $job->job_number() . " processors $new_processors\n";
				##DEBUG_END

				$job->assign($self->{current_time}, $new_processors);
				$self->{execution_profile}->add_job($self->{current_time}, $job, $self->{current_time});
			} else {
				$self->{execution_profile}->add_job($job_starting_time, $job, $self->{current_time});
			}
		}
	}

	return;
}

sub assign_job {
	my $self = shift;
	my $job = shift;

	##DEBUG_BEGIN
	#print STDERR "assigning job " . $job->job_number() . "\n";
	##DEBUG_END

	my ($starting_time, $chosen_processors) = $self->{execution_profile}->find_first_profile($job);

	##DEBUG_BEGIN
	print STDERR "chose starting time $starting_time and processors $chosen_processors duration " . $job->requested_time() . " for job " . $job->job_number() . "\n";
	##DEBUG_END

	# here we can decide the new run time based on the platform level
	if (defined $self->{platform}->speedup()) {
		my $job_platform_level = $self->{platform}->job_relative_level_distance($chosen_processors, $job->requested_cpus());
		my $new_job_run_time = int($job->run_time() + $job->run_time() * $self->{communication_level} * $self->{platform}->speedup($job_platform_level - 1));

		if ($new_job_run_time >= $job->requested_time()) {
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
