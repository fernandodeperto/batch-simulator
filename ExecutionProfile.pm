package ExecutionProfile;
use parent 'Displayable';

use strict;
use warnings;

use List::Util qw(min max);
use Data::Dumper;
use Carp;

use Util qw(float_equal float_precision);
use Profile;
use BinarySearchTree;
use Platform;
use ProcessorRange;

use Debug;

use overload '""' => \&stringification;

sub new {
	my $class = shift;
	my $processors_number = shift;
	my $reduction_algorithm = shift;

	confess unless defined $processors_number;

	my $self = {
		processors_number => $processors_number,
		reduction_algorithm => $reduction_algorithm,
	};

	$self->{profile_tree} = BinarySearchTree->new(-1, 0);
	$self->{profile_tree}->add_content(Profile->new(0, undef, [0, $self->{processors_number} - 1]));

	bless $self, $class;
	return $self;
}

sub get_free_processors {
	my $self = shift;
	my $job = shift;
	my $starting_time = shift;

	my $duration = 0;

	my $left_processors = ProcessorRange->new(0, $self->{processors_number} - 1);
	my $requested_time = $job->requested_time();

	$self->{profile_tree}->nodes_loop($starting_time, undef,
		sub {
			my $profile = shift;

			# stop if we have enough profiles
			return 0 if ($duration >= $requested_time);

			# profiles must all be contiguous
			return 0 unless float_equal($starting_time + $duration, $profile->starting_time());

			$left_processors->intersection($profile->processors());
			return 0 if $left_processors->size() < $job->requested_cpus();

			$duration = (defined $profile->ending_time()) ? $duration + $profile->duration() : $requested_time;
			return 1;
		});

	# check if the nodes_loop section found all needed CPUs
	if ($left_processors->size() < $job->requested_cpus() or (not float_equal($duration, $requested_time) and $duration < $requested_time)) {
		$left_processors->free_allocated_memory();
		return;
	}

	$self->{reduction_algorithm}->reduce($job, $left_processors);

	unless ($left_processors->size()) {
		$left_processors->free_allocated_memory();
		return;
	}


	return $left_processors;
}

sub available_processors {
	my $self = shift;
	my $starting_time = shift;

	my $profile = $self->{profile_tree}->find_content($starting_time);
	return $profile->processors()->processors_ids() if defined $profile;
}

sub remove_job {
	my $self = shift;
	my $job = shift;
	my $current_time = shift;

	my $starting_time = $job->starting_time();
	my $job_ending_time = $job->submitted_ending_time();

	my @impacted_profiles;
	Profile::set_comparison_function('all_times');
	$self->{profile_tree}->nodes_loop($starting_time, $job_ending_time,
		sub {
			my $profile = shift;
			push @impacted_profiles, $profile;
			return 1;
		}
	);
	Profile::set_comparison_function('default');

	unless (@impacted_profiles) {
		# avoid starting in the past
		my $start = max($current_time, $starting_time);

		# only remove if it is still there
		if (not float_equal($job_ending_time, $start) and $job_ending_time > $start) {
			my $new_profile = Profile->new(
				$start,
				$job_ending_time,
				$job->assigned_processors()->copy_range()
			);
			$self->{profile_tree}->add_content($new_profile);
		}
		return;
	}

	# split at the first profile
	if (not float_equal($impacted_profiles[0]->starting_time(), $starting_time) and $impacted_profiles[0]->starting_time() < $starting_time) {
		# remove
		my $first_profile = shift @impacted_profiles;
		$self->{profile_tree}->remove_content($first_profile);

		# split in two
		my $first_profile_ending_time = $first_profile->ending_time();
		$first_profile->ending_time($starting_time);
		my $second_profile = Profile->new(
			$starting_time,
			$first_profile_ending_time,
			$first_profile->processors()->copy_range()
		);

		# put back
		$self->{profile_tree}->add_content($first_profile);
		$self->{profile_tree}->add_content($second_profile);
		unshift @impacted_profiles, $second_profile;
	}

	# split at the last profile
	if ($impacted_profiles[-1]->ends_after($job_ending_time)) {
		# remove
		my $first_profile = pop @impacted_profiles;
		$self->{profile_tree}->remove_content($first_profile);

		# split in two
		my $second_profile = Profile->new(
			$job_ending_time,
			$first_profile->ending_time(),
			$first_profile->processors()->copy_range()
		);
		$first_profile->ending_time($job_ending_time);

		# put back
		$self->{profile_tree}->add_content($first_profile);
		$self->{profile_tree}->add_content($second_profile);
		push @impacted_profiles, $first_profile;
	}

	# update profiles
	my $previous_profile_ending_time = max($starting_time, $current_time);
	for my $profile (@impacted_profiles) {
		$profile->remove_job($job);

		if (not float_equal($profile->starting_time(), $previous_profile_ending_time) and $profile->starting_time() > $previous_profile_ending_time) {
			my $new_profile = Profile->new(
				$previous_profile_ending_time,
				$profile->starting_time(),
				$job->assigned_processors()->copy_range()
			);
			$self->{profile_tree}->add_content($new_profile);
		}
		$previous_profile_ending_time = $profile->ending_time();
	}

	# gap at the end
	if (not float_equal($job_ending_time, $previous_profile_ending_time) and $job_ending_time > $previous_profile_ending_time) {
		my $new_profile = Profile->new(
			$previous_profile_ending_time,
			$job_ending_time,
			$job->assigned_processors()->copy_range()
		);
		$self->{profile_tree}->add_content($new_profile);
	}

	return;
}

sub add_job  {
	my $self = shift;
	my $starting_time = shift;
	my $job = shift;

	my @profiles_to_update;
	my $ending_time = $starting_time + $job->requested_time();

	$self->{profile_tree}->nodes_loop($starting_time, $ending_time,
		sub {
			my $profile = shift;

			# avoid including a profile that starts at $ending_time
			return 0 if (float_equal($profile->starting_time, $ending_time));

			push @profiles_to_update, $profile;
			return 1;
		});

	for my $profile (@profiles_to_update) {
		$self->{profile_tree}->remove_content($profile);
		my @new_profiles = $profile->add_job($job);
		$profile->processors()->free_allocated_memory();
		$self->{profile_tree}->add_content($_) for (@new_profiles);
	}

	return;
}

sub could_start_job {
	my $self = shift;
	my $job = shift;
	my $starting_time = shift;

	my $min_processors = $job->requested_cpus();
	my $job_ending_time = $starting_time + $job->requested_time();

	$self->{profile_tree}->nodes_loop($starting_time, undef,
		sub {
			my $profile = shift;

			# gap in the profile, can't use it to run the job
			unless (float_equal($starting_time, $profile->starting_time())) {
				$min_processors = 0;
				return 0;
			}

			# ok to return if it's the last profile
			return 0 unless defined $profile->duration();

			$starting_time += $profile->duration();
			$min_processors = min($min_processors, $profile->processors()->size());

			# ok to return, profile may be good for the job
			return 0 if float_equal($starting_time, $job_ending_time) or $starting_time > $job_ending_time;
			return 0 if $min_processors <= $job->requested_cpus();
			return 1;
		});

	return $min_processors >= $job->requested_cpus();
}

sub find_first_profile {
	my $self = shift;
	my $job = shift;

	# used to return the results
	my $starting_time;
	my $processors;

	# used to keep track of the starting times
	my @included_profiles;
	my $previous_ending_time;

	$self->{profile_tree}->nodes_loop(undef, undef,
		sub {
			my $profile = shift;

			# gap in the list of profiles
			@included_profiles = () if defined $previous_ending_time and not float_equal($previous_ending_time, $profile->starting_time());

			$previous_ending_time = $profile->ending_time();
			push @included_profiles, $profile;

			# not enough processors to continue
			@included_profiles = () if $profile->processors()->size() < $job->requested_cpus();

			while (@included_profiles and (not defined $included_profiles[-1]->ending_time() or $included_profiles[-1]->ending_time() - $included_profiles[0]->starting_time() > $job->requested_time())) {
				my $start_profile = shift @included_profiles;

				$starting_time = $start_profile->starting_time();
				$processors = $self->get_free_processors($job, $start_profile->starting_time());
				return 0 if defined $processors;
			}

			return 1;
		});

	return ($starting_time, $processors) if defined $processors;
	return;
}

sub set_current_time {
	my $self = shift;
	my $current_time = shift;

	my $updated_profile;
	my @removed_profiles;

	$self->{profile_tree}->nodes_loop(undef, $current_time,
		sub {
			my $profile = shift;

			return 0 if float_equal($profile->starting_time(), $current_time);

			my $starting_time = $profile->starting_time();
			my $ending_time = $profile->ending_time();

			if (not defined $ending_time or (not float_equal($ending_time, $current_time) and $ending_time > $current_time)) {
				$updated_profile = $profile;
				return 0;
			}

			push @removed_profiles, $profile;
			return 1;
		});

	if (defined $updated_profile) {
		$self->{profile_tree}->remove_content($updated_profile);
		$updated_profile->starting_time($current_time);
		$self->{profile_tree}->add_content($updated_profile);
	}

	for my $profile (@removed_profiles) {
		$self->{profile_tree}->remove_content($profile);
		$profile->processors()->free_allocated_memory();
	}

	return;
}

sub free_profiles {
	my $self = shift;
	my @profiles;

	$self->{profile_tree}->nodes_loop(undef, undef,
		sub {
			my $profile = shift;
			push @profiles, $profile;
			return 1;
		}
	);

	for my $profile (@profiles) {
		$self->{profile_tree}->remove_content($profile);
		$profile->processors()->free_allocated_memory();
	}
	return;
}

sub stringification {
	my $self = shift;
	my @profiles;

	$self->{profile_tree}->nodes_loop(undef, undef,
		sub {
			my $profile = shift;
			push @profiles, $profile;
			return 1;
		});

	return join("\n", @profiles);
}

sub save_svg {
	my ($self, $svg_filename, $time) = @_;
	$time = 0 unless defined $time;

	my @profiles;
	$self->{profile_tree}->nodes_loop(undef, undef,
		sub {
			my $profile = shift;
			push @profiles, $profile;
			return 1;
		});

	my $last_starting_time = $profiles[-1]->starting_time();
	return if float_equal($last_starting_time, 0);

	open(my $filehandle, '>', "$svg_filename") or die "unable to open $svg_filename";

	print $filehandle "<svg width=\"800\" height=\"600\">\n";
	my $w_ratio = 800/$last_starting_time;
	my $h_ratio = 600/$self->{processors_number};

	# red line at the current time
	my $current_x = $w_ratio * $time;
	print $filehandle "<line x1=\"$current_x\" x2=\"$current_x\" y1=\"0\" y2=\"600\" style=\"stroke:rgb(255,0,0);stroke-width:5\"/>\n";

	for my $profile_index (0..($#profiles-1)) {
		$profiles[$profile_index]->svg($filehandle, $w_ratio, $h_ratio, $time, $profile_index);
	}

	print $filehandle "</svg>\n";
	close $filehandle;
	return;
}

1;
