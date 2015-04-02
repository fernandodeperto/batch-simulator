package Trace;
use strict;
use warnings;
use JSON;
use List::Util qw(max reduce sum);
use List::MoreUtils qw(natatime);
use Storable qw(dclone);
use POSIX qw(ceil floor);
use Log::Log4perl qw(get_logger);

use Job;

sub new {
	my $class = shift;
	my $self = { jobs => [] };
	bless $self, $class;
	return $self;
}

sub save_json {
    my $self = shift;
    my $json;
    $json->{version} = 0;
    $json->{command} = "";
    $json->{date} = `date -R`;
    $json->{description} = "Auto-generated from trace $self->{filename}";

    $json->{nb_res} = int(shift);    
    $json->{profiles} = {};
    $json->{jobs} = [];
   
    my $job_number = 1;
    for my $job (@{$self->{jobs}}) {
	my $id = $job_number;
	push @{$json->{jobs}}, {
	    'id' => $job_number,
	    'subtime' => int($job->submit_time()),
	    'walltime' => int($job->requested_time()),
	    'res' => int($job->requested_cpus()),
	    'profile' => "p$id",
	};
	
	$json->{profiles}->{"p$id"} = {
	    'type' => 'msg_par_hg',
	    'cpu' => int($job->run_time()*10000000),
	    'com' => 1000000,
	};
	
	$job_number++;
    }
	

    my $json_text = to_json( $json, { pretty => 1, canonical => 1 } );

    my $file = shift;
    open(my $fd, '>', $file) or die "not open possible for $file";

    print $fd "$json_text\n";
    close $fd;
    return;
}

sub add_job {
	my $self = shift;
	push @{$self->{jobs}}, shift;
	return;
}

sub new_from_swf {
	my $class = shift;
	my $filename = shift;
	my $jobs_number = shift;
	my $logger = get_logger('Trace::new');

	my $self = {
		filename => $filename,
		jobs => [],
		status => []
	};

	open (my $file, '<', $self->{filename}) or $logger->logdie("unable to open $self->{filename}");

	while (defined(my $line = <$file>) and (not defined $jobs_number or @{$self->{jobs}} < $jobs_number)) {
		my @fields = split(' ', $line);

		next unless defined $fields[0];

		# Status line
		if ($fields[0] =~/^;/) {
			push @{$self->{status}}, [@fields];
		}

		# Job line
		elsif ($fields[0] ne ' ') {
			my $job = Job->new(@fields);
			push @{$self->{jobs}}, $job;
		}
	}
	close($file);

	bless $self, $class;
	return $self;
}

sub keep_first_jobs {
	my $self = shift;
	my $jobs_number = shift;
	my $end = $jobs_number - 1;
	my $last_available = $#{$self->{jobs}};

	$end = $last_available if $last_available < $end;
	@{$self->{jobs}} = @{$self->{jobs}}[0..$end];

	return;
}

sub reset_requested_times {
	my $self = shift;

	$_->{requested_time} = $_->{run_time} for @{$self->{jobs}};

	return;
}

sub fix_submit_times {
	my $self = shift;
	my $start = $self->{jobs}->[0]->submit_time();

	$_->submit_time($_->submit_time() - $start) for @{$self->{jobs}};

	return;
}

sub new_block_from_trace {
	my $class = shift;
	my $trace = shift;
	my $size = shift;
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
	my $class = shift;
	my $trace = shift;
	my $size = shift;
	my $logger = get_logger('Trace::new_from_trace');

	$logger->logdie('empty trace') unless defined $trace->{jobs}->[0];

	my $self = {
		jobs => []
	};

	push @{$self->{jobs}}, dclone($trace->{jobs}->[int rand(@{$trace->{jobs}})]) for (1..$size);

	bless $self, $class;
	return $self;
}

sub copy_from_trace {
	my $class = shift;
	my $trace = shift;

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
	my $class = shift;
	my $original = shift;

	my $self = {
		jobs => []
	};

	push @{$self->{jobs}}, Job->copy($_) for @{$original->{jobs}};

	bless $self, $class;
	return $self;
}


sub reset_submit_times {
	my $self = shift;

	$_->submit_time(0) for (@{$self->{jobs}});

	return;
}

sub reset_jobs_numbers {
	my $self = shift;

	$self->{jobs}->[$_ - 1]->job_number($_) for (1..(@{$self->{jobs}}));

	return;
}

sub write_to_file {
	my $self = shift;
	my $trace_file_name = shift;

	open(my $filehandle, '>', "$trace_file_name") or die "unable to open $trace_file_name";
	print $filehandle "$_\n" for (@{$self->{jobs}});
	close($filehandle);

	return;
}

sub needed_cpus {
	my $self = shift;
	return max map {$_->requested_cpus()} @{$self->{jobs}};
}

sub jobs {
	my $self = shift;
	my $jobs = shift;

	$self->{jobs} = $jobs if defined $jobs;

	return $self->{jobs};
}

sub job {
	my $self = shift;
	my $job_number = shift;

	return $self->{jobs}->[$job_number];
}

sub remove_large_jobs {
	my $self = shift;
	my $limit = shift;

	my @left_jobs = grep {$_->requested_cpus() <= $limit} @{$self->{jobs}};
	$self->{jobs} = [@left_jobs];

	return;
}

sub unassign_jobs {
	my $self = shift;

	$_->unassign() for @{$self->{jobs}};

	return;
}

sub load {
	my $self = shift;
	my $processors_number = shift;
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
