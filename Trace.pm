package Trace;
use strict;
use warnings;

use Data::Dumper qw(Dumper);
use List::Util qw(max reduce sum);
use List::MoreUtils qw(natatime);
use Storable qw(dclone);
use POSIX qw(ceil floor);

use Job;
use Database;

sub new_from_swf {
	my $class = shift;
	my $self = {
		file => shift,
		jobs => [],
		status => [],
		needed_cpus => 0
	};

	open (FILE, $self->{file}) or die "unable to open $self->{file}";

	while (my $line = <FILE>) {
		my @fields = split(' ', $line);

		next unless defined $fields[0];

		# Status line
		if ($fields[0] eq ';') {
			push @{$self->{status}}, [@fields];
		}

		# Job line
		elsif ($fields[0] ne ' ') {
			my $job = new Job(@fields);
			$self->{needed_cpus} = max($self->{needed_cpus}, $job->requested_cpus);
			push @{$self->{jobs}}, $job;
		}
	}

	bless $self, $class;
	return $self;
}

sub fix_submit_times {
	my $self = shift;

	return if (!$self->{jobs}[0]->submit_time());

	my $first_submit_time = $self->{jobs}[0]->submit_time();
	$_->submit_time($_->submit_time() - $first_submit_time) for @{$self->{jobs}};
}

sub new_block_from_trace {
	my ($class, $trace, $size) = @_;

	my $self = {
		needed_cpus => 0
	};

	my $start_point = int(rand(scalar @{$trace->jobs()} - $size + 1));
	my @selected_jobs = @{$trace->jobs()}[$start_point..($start_point + $size - 1)];

	$self->{jobs} = [@selected_jobs];
	$self->{needed_cpus} = max map {$_->requested_cpus} @{$self->{jobs}};

	bless $self, $class;
	return $self;
}

sub new_from_trace {
	my ($class, $trace, $size) = @_;

	my $self = {
		jobs => [],
		needed_cpus => 0
	};

	die "creating from empty trace" unless defined $trace->{jobs}->[0];

	for my $job_number (1..$size) {
		my $job_id = int(rand(@{$trace->{jobs}}));
		my $new_job = dclone($trace->{jobs}->[$job_id]);

		$self->{needed_cpus} = max($self->{needed_cpus}, $new_job->requested_cpus);
		push @{$self->{jobs}}, $new_job;
	}

	bless $self, $class;
	return $self;
}

sub copy_from_trace {
	my ($class, $trace) = @_;

	my $self = {
		jobs => [],
		needed_cpus => $trace->needed_cpus()
	};

	for my $job (@{$trace->jobs()}) {
		my $new_job = dclone($job);
		push @{$self->{jobs}}, $new_job;
	}

	bless $self, $class;
	return $self;
}


sub new_from_database {
	my ($class, $trace_id) = @_;

	my $self = {
		jobs => [],
		needed_cpus => 0
	};

	my $database = Database->new();
	my $trace_ref = $database->get_trace_ref($trace_id);
	my @job_refs = $database->get_job_refs($trace_id);

	for my $job_ref (@job_refs) {
		my $job = Job->new(
			$job_ref->{job_number},
			$job_ref->{submit_time},
			$job_ref->{wait_time},
			$job_ref->{run_time},
			$job_ref->{allocated_cpus},
			$job_ref->{avg_cpu_time},
			$job_ref->{used_mem},
			$job_ref->{requested_cpus},
			$job_ref->{requested_time},
			$job_ref->{requested_mem},
			$job_ref->{status},
			$job_ref->{uid},
			$job_ref->{gid},
			$job_ref->{exec_number},
			$job_ref->{queue_number},
			$job_ref->{partition_number},
			$job_ref->{prec_job_number},
			$job_ref->{think_time_prec_job}
		);

		$self->{needed_cpus} = max($self->{needed_cpus}, $job->requested_cpus);
		push @{$self->{jobs}}, $job;
	}

	bless $self, $class;
	return $self;
}

sub copy {
	my $class = shift;
	my $original = shift;
	my $self = {
		jobs => []
	};
	push @{$self->{jobs}}, copy Job($_) for @{$original->{jobs}};
	$self->{needed_cpus} = $original->{needed_cpus};
	bless $self, $class;
	return $self;
}


sub reset_submit_times {
	my ($self) = @_;
	$_->submit_time(0) for (@{$self->{jobs}});
}

sub reset_jobs_numbers {
	my ($self) = @_;

	for my $i (0..$#{$self->{jobs}}) {
		$self->{jobs}[$i]->job_number($i + 1);
	}
}

sub write_to_file {
	my ($self, $trace_file_name) = @_;

	open(my $filehandle, "> $trace_file_name") or die "unable to open $trace_file_name";

	for my $job (@{$self->{jobs}}) {
		print $filehandle "$job\n";
	}
}

sub needed_cpus {
	my ($self, $needed_cpus) = @_;
	$self->{needed_cpus} = $needed_cpus if defined $needed_cpus;
	return $self->{needed_cpus};
}

sub print_jobs {
	my ($self) = @_;
	print join("\n", @{$self->{jobs}});
}

sub jobs {
	my ($self, $jobs) = @_;
	$self->{jobs} = $jobs if defined $jobs;
	return $self->{jobs};
}

sub job {
	my ($self, $job_number) = @_;
	return $self->{jobs}[$job_number];
}

sub number_of_jobs {
	my ($self) = @_;
	return scalar @{$self->{jobs}};
}

sub remove_large_jobs {
	my ($self, $limit) = @_;
	die unless defined $limit;
	my @left_jobs = grep {$_->requested_cpus() <= $limit} @{$self->{jobs}};
	$self->{jobs} = [@left_jobs];
}

sub reset {
	my ($self) = @_;
	$_->reset() for @{$self->{jobs}};
	$self->{needed_cpus} = 0;
}

sub file {
	my ($self) = @_;
	return $self->{file};
}

sub characteristic {
	my ($self, $characteristic_id, $cpus_number) = @_;

	if ($characteristic_id == 0) {
		return scalar grep {$_->requested_cpus() > $cpus_number/2} @{$self->{jobs}};
	}

	elsif ($characteristic_id == 1) {
		my $piece_size = shift;
		my $pieces_with_large_jobs = 0;

		my $it = natatime $piece_size, @{$self->{jobs}};
		while (my @piece = $it->()) {
			my $large_jobs_number = scalar grep {$_->requested_cpus() > $cpus_number/2} @piece;
			$pieces_with_large_jobs++ if $large_jobs_number > 0;
		}

		return $pieces_with_large_jobs;
	}

	# Find the ammount of wasted work in the trace based on the largest job and the bumber of required CPUs
	elsif ($characteristic_id == 2) {
		my $longest_duration = 0;
		my $work = 0;
		my $worst_wasted_work = 0;
		for my $job (@{$self->{jobs}}) {
			my $wasted_work = ($cpus_number-$job->requested_cpus()) * $longest_duration - $work;
			$worst_wasted_work = $wasted_work if $wasted_work > $worst_wasted_work;
			$longest_duration = $job->run_time() if $job->run_time() > $longest_duration;
			$work += $job->requested_cpus() * $job->run_time();
		}
		my $backfilling_waste = $cpus_number * $longest_duration - $work;
		my $difference = $worst_wasted_work - $backfilling_waste;
		return 0 if $difference < 0;
		return ($difference / $work);
	}

	# Find the ammount of wasted work in the trace between large jobs
	# The distance between large jobs is based on the size of the longest job between them
	elsif ($characteristic_id == 3) {
		my $longest_duration = 0;
		my $work = 0;
		my $total_work = 0;
		my $total_wasted_work = 0;
		my $total_maximum_work = 0;
		my $total_length = 0;

		for my $job (@{$self->{jobs}}) {
			$work += $job->requested_cpus() * $job->run_time();
			$longest_duration = $job->run_time() if $job->run_time() > $longest_duration;

			if ($job->requested_cpus() >= $cpus_number/2) {
				$total_work += $work;
				$total_maximum_work += $longest_duration * $cpus_number;

				$longest_duration = 0;
				$work = 0;
			}
		}

		if ($work) {
			$total_work += $work;
			$total_maximum_work += $longest_duration * $cpus_number;
		}

		return $total_work/$total_maximum_work;
	}
}

sub load {
	my ($self, $processors_number) = @_;

	my $jobs_number = scalar @{$self->{jobs}};

	my $first_job_index = floor($jobs_number * 0.01);
	my $first_job = $self->{jobs}->[$first_job_index];
	my $t_start = $first_job->submit_time() + $first_job->wait_time();
	my @valid_jobs = @{$self->{jobs}}[$first_job_index..$#{$self->{jobs}}];

	my $last_submit_time = $self->{jobs}->[$#{$self->{jobs}}]->submit_time();
	@valid_jobs = grep {$_->submit_time() + $_->wait_time() + $_->run_time() < $last_submit_time} @valid_jobs;
	my $t_end = max map {$_->submit_time() + $_->wait_time() + $_->run_time()} @valid_jobs;

	my $load = sum map {$_->requested_cpus() * $_->run_time() / ($processors_number * ($t_end - $t_start))} @valid_jobs;
	return $load;
}

1;
