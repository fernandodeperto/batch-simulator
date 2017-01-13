package Trace;
use strict;
use warnings;

use JSON;
use List::Util qw(max reduce sum min);
use List::MoreUtils qw(natatime);
use Storable qw(dclone);
use POSIX qw(ceil floor);
use Clone qw(clone);
use Data::Dumper qw(Dumper);

use Job;

sub new {
	my ($class) = @_;

	my $self = {
		jobs => []
	};

	bless $self, $class;
	return $self;
}

sub save_json {
	my ($self, $file, $cpus_number, $comm_factor, $comp_factor) = @_;

	my $json = {
		version => 0,
		command => "",
		date => `date -R`,
		escription => "Auto-generated from trace $self->{filename}",
		nb_res => int($cpus_number),
		profiles => {},
		jobs => [],
	};

	my $job_number = 1;
	for my $job (@{$self->{jobs}}) {
		my $id = $job->job_number();

		push @{$json->{jobs}}, {
			'id' => int($id),
			'subtime' => int($job->submit_time()),
			'walltime' => int($job->requested_time()),
			'res' => int($job->requested_cpus()),
			'profile' => "p$id",
			'original_wait_time' => $job->original_wait_time(),
		};

		$json->{profiles}->{"p$id"} = {
			'type' => 'msg_par_hg',
			'cpu' => int($job->run_time() * $comp_factor),
			'com' => int($job->run_time() * $comm_factor),
		};
	}

	my $json_text = to_json( $json, { pretty => 1, canonical => 1 } );
	open(my $fd, '>', $file) or die "not open possible for $file";
	print $fd "$json_text\n";

	close $fd;
	return;
}

sub add_job {
	my ($self, $job) = @_;

	push @{$self->{jobs}}, $job;

	return;
}

sub new_from_swf {
	my ($class, $filename) = @_;

	my $self = {
		filename => $filename,
		jobs => [],
		status => []
	};

	open (my $file, '<', $self->{filename}) or die("unable to open $self->{filename}");

	while (defined(my $line = <$file>)) {
		my @fields = split(' ', $line);

		next unless defined $fields[0];

		# Status or comment line
		if ($fields[0] =~/^;/) {
			#push @{$self->{status}}, [@fields];
			next;
		}

		# Job line
		elsif ($fields[0] ne ' ') {
			my $job = Job->new(@fields);
			next if ($job->status() != 1);
			push @{$self->{jobs}}, $job;
		}
	}
	close($file);

	bless $self, $class;
	return $self;
}

sub keep_first_jobs {
	my ($self, $jobs_number) = @_;

	$self->{jobs} = [@{$self->{jobs}}[0..($jobs_number - 1)]] if $jobs_number < scalar @{$self->{jobs}};

	return;
}

sub reset_requested_times {
	my ($self) = @_;

	$_->{requested_time} = $_->{run_time} for @{$self->{jobs}};

	return;
}

sub fix_submit_times {
	my ($self) = @_;

	my $start = $self->{jobs}->[0]->submit_time();

	$_->submit_time($_->submit_time() - $start) for @{$self->{jobs}};

	return;
}

sub new_block_from_trace {
	my ($class, $trace, $size) = @_;

	my $start_point = int(rand(scalar @{$trace->jobs()} - $size + 1));
	my $end_point = $start_point + $size - 1;
	my @selected_jobs = @{$trace->jobs()}[$start_point..$end_point];

	my $self = {
		jobs => [@selected_jobs]
	};

	bless $self, $class;
	return $self;
}

sub new_from_trace {
	my ($class, $trace, $size) = @_;

	die 'empty trace' unless defined $trace->{jobs}->[0];

	my $self = {
		jobs => [],
		filename => $trace->{filename},
	};

	push @{$self->{jobs}}, dclone($trace->{jobs}->[int rand(@{$trace->{jobs}})]) for (1..$size);

	bless $self, $class;
	return $self;
}

sub copy_from_trace {
	my ($class, $trace) = @_;

	my $self = {
		jobs => []
	};

	for my $job (@{$trace->jobs()}) {
		my $new_job = dclone($job);
		push @{$self->{jobs}}, $new_job;
	}

	bless $self, $class;
	return $self;
}

sub copy {
	my ($class, $original) = @_;

	my $self = {
		jobs => []
	};

	push @{$self->{jobs}}, Job->copy($_) for @{$original->{jobs}};

	bless $self, $class;
	return $self;
}

sub copy_random_block {
	my ($class, $trace, $jobs_number) = @_;

	my $starting_point = int(rand(scalar @{$trace->{jobs}} - $jobs_number));
	my @chosen_jobs = map {clone $trace->{jobs}->[$_]} ($starting_point..($starting_point + $jobs_number - 1));

	my $self = {
		jobs => [sort {$a->submit_time() <=> $b->submit_time()} @chosen_jobs],
		submitted_jobs => [],
	};

	bless $self, $class;
	return $self;
}

sub copy_random_time_period {
	my ($class, $trace, $block_size) = @_;

	my $first_submit_time = min map {$_->submit_time()} @{$trace->{jobs}};
	my $last_submit_time = max map {$_->submit_time()} @{$trace->{jobs}};

	my @new_jobs;

	do {
		my $starting_point = $first_submit_time + int rand($last_submit_time - $block_size - $first_submit_time);
		@new_jobs = map {clone $_} grep {$_->submit_time() >= $starting_point and $_->submit_time() < $starting_point + $block_size} @{$trace->{jobs}};
	} until @new_jobs;

	my $self = {
		jobs => [sort {$a->submit_time() <=> $b->submit_time()} @new_jobs],
		submitted_jobs => [],
	};

	bless $self, $class;
	return $self;
}

sub remove_submit_times {
	my ($self) = @_;

	$_->submit_time(0) for (@{$self->{jobs}});

	return;
}

sub reset_jobs_numbers {
	my ($self) = @_;

	$self->{jobs}->[$_ - 1]->job_number($_) for (1..(@{$self->{jobs}}));

	return;
}

sub write {
	my ($self, $trace_filename) = @_;

	open(my $filehandle, '>', "$trace_filename") or die "unable to open $trace_filename";

	print $filehandle "$_\n" for (@{$self->{jobs}});

	close($filehandle);
	return;
}

sub needed_cpus {
	my ($self) = @_;
	return max map {$_->requested_cpus()} @{$self->{jobs}};
}

sub jobs {
	my ($self, $jobs) = @_;

	$self->{jobs} = $jobs if defined $jobs;

	return $self->{jobs};
}

sub job {
	my ($self, $job_number) = @_;

	return $self->{jobs}->[$job_number];
}

sub remove_large_jobs {
	my ($self, $limit) = @_;

	my @left_jobs = grep {$_->requested_cpus() <= $limit} @{$self->{jobs}};
	$self->{jobs} = [@left_jobs];

	return;
}

sub unassign_jobs {
	my ($self) = @_;

	$_->unassign() for @{$self->{jobs}};

	return;
}

sub load {
	my ($self, $processors_number) = @_;

	my $jobs_number = scalar @{$self->{jobs}};
	my $first_job_index = floor($jobs_number * 0.01);
	my $first_job = $self->{jobs}->[$first_job_index];
	my $t_start = $first_job->submit_time() + $first_job->wait_time();
	my @valid_jobs = @{$self->{jobs}}[$first_job_index..$#{$self->{jobs}}];
	my $last_submit_time = $self->{jobs}->[-1]->submit_time();

	@valid_jobs = grep {$_->submit_time() + $_->wait_time() + $_->run_time() < $last_submit_time} @valid_jobs;
	my $t_end = max map {$_->submit_time() + $_->wait_time() + $_->run_time()} @valid_jobs;
	my $load = sum map {$_->requested_cpus() * $_->run_time() / ($processors_number * ($t_end - $t_start))} @valid_jobs;

	return $load;
}

sub normalize_run_times {
	my ($self, $factor) = @_;

	$_->run_time(max(int($_->requested_time()/$factor), $_->run_time())) for (@{$self->{jobs}});

	return;
}

sub normalize_requested_times {
	my ($self, $factor) = @_;

	$_->requested_time($_->run_time() * $factor) for (@{$self->{jobs}});

	return;
}

1;
