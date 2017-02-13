package Profile;

use strict;
use warnings;

use POSIX;
use Scalar::Util qw(blessed);
use Data::Dumper qw(Dumper);
use Carp;
use List::Util qw(min);

use overload
	'""' => \&stringification,
	'<=>' => \&three_way_comparison,
	'-' => \&subtract,
;

sub new {
	my ($class, $starting_time, $ending_time, $ids) = @_;

	my $self = {
		starting_time => $starting_time,
		ending_time => $ending_time,
	};

	die "invalid profile duration ($self->{ending_time} - $self->{starting_time}" if defined $self->{ending_time} and $self->{ending_time} <= $self->{starting_time};

	$self->{processors} = (defined blessed($ids) and blessed($ids) eq 'ProcessorRange') ? $ids : ProcessorRange->new(@$ids);

	bless $self, $class;
	return $self;
}

sub stringification {
	my ($self) = @_;

	my $cpus_number = scalar $self->{processors}->processors_ids();

	return "[$self->{starting_time} ; $cpus_number = ($self->{processors}) " . (defined $self->{ending_time} ? ": $self->{ending_time}]" : "]");
}

sub processors {
	my ($self, $processors) = @_;

	$self->{processors} = $processors if defined $processors;

	return $self->{processors};
}

sub processors_ids {
	my ($self) = @_;
	return $self->{processors}->processors_ids();
}

sub duration {
	my ($self) = @_;

	return $self->{ending_time} - $self->{starting_time} if defined $self->{ending_time};
	return;
}

sub ending_time {
	my ($self, $ending_time) = @_;

	$self->{ending_time} = $ending_time if defined $ending_time;

	return $self->{ending_time};
}

sub add_job {
	my ($self, $job) = @_;

	return $self->split_by_job($job);
}

sub remove_job {
	my ($self, $job) = @_;

	$self->{processors}->add($job->assigned_processors());

	return;
}

sub split_by_job {
	my ($self, $job) = @_;

	my @profiles;

	my $middle_start = $self->{starting_time};
	my $middle_end = (defined $self->{ending_time}) ? min($self->{ending_time}, $job->submitted_ending_time()) : $job->submitted_ending_time();
	my $middle_profile = Profile->new($middle_start, $middle_end, $self->{processors}->copy_range());
	
	$middle_profile->{processors}->remove($job->assigned_processors());

	if ($middle_profile->{processors}->size()) {
		push @profiles, $middle_profile
	} else {
		$middle_profile->processors()->free_allocated_memory();
	}

	if (not defined $self->{ending_time} or $job->submitted_ending_time() < $self->{ending_time}) {
		my $end_profile = Profile->new($job->submitted_ending_time(), $self->{ending_time}, $self->{processors}->copy_range());
		push @profiles, $end_profile;
	}

	return @profiles;
}

sub remove_processors {
	my ($self, $job) = @_;

	my $assigned_processors_ids = $job->assigned_processors_ids();
	$self->{processors}->remove($assigned_processors_ids);

	return;
}

sub starting_time {
	my ($self, $starting_time) = @_;

	$self->{starting_time} = $starting_time if defined $starting_time;

	return $self->{starting_time};
}

sub ends_after {
	my ($self, $time) = @_;

	return 1 unless defined $self->{ending_time};
	return $self->{ending_time} > $time;
}

sub svg {
	my ($self, $fh, $w_ratio, $h_ratio, $current_time, $index) = @_;

	my @svg_colors = qw(red green blue purple orange saddlebrown mediumseagreen darkolivegreen darkred dimgray mediumpurple midnightblue olive chartreuse darkorchid hotpink lightskyblue peru goldenrod mediumslateblue orangered darkmagenta darkgoldenrod mediumslateblue);

	$self->{processors}->ranges_loop(
		sub {
			my ($start, $end) = @_;

			#rectangle
			my $x = $self->{starting_time} * $w_ratio;
			my $w = $self->duration() * $w_ratio;

			my $y = $start * $h_ratio;
			my $h = $h_ratio * ($end - $start + 1);
			my $color = $svg_colors[$index % @svg_colors];
			my $sw = min($w_ratio, $h_ratio) / 10;
			$w = 1 if $w < 1;
			print $fh "\t<rect x=\"$x\" y=\"$y\" width=\"$w\" height=\"$h\" style=\"fill:$color;fill-opacity:0.2;stroke:black;stroke-width:$sw\"/>\n";
			return 1;
		}
	);
	return;
}

# Comparison functions

my $comparison_function = 'default';
my %comparison_functions = (
	'default' => \&starting_times_comparison,
	'all_times' => \&all_times_comparison
);

sub set_comparison_function {
	($comparison_function) = @_;
	return;
}

sub three_way_comparison {
	my ($self, $other, $inverted) = @_;

	return $comparison_functions{$comparison_function}->($self, $other, $inverted);
}

sub starting_times_comparison {
	my ($self, $other, $inverted) = @_;

	# Save two calls to the comparison functions if $other is a Profile
	$other = $other->starting_time() if defined blessed($other) and blessed($other) eq 'Profile';

	return $other <=> $self->{starting_time} if $inverted;
	return $self->{starting_time} <=> $other;
}

sub all_times_comparison {
	my ($self, $other, $inverted) = @_;

	return $self->{starting_time} <=> $other->{starting_time} if defined blessed($other) and blessed($other) eq 'Profile';

	my $coef = $inverted ? -1 : 1;

	return -$coef if defined $self->{ending_time} and $self->{ending_time} <= $other;
	return $coef if $self->{starting_time} >= $other;
	return 0;
}

sub subtract {
	my ($self, $other, $inverted) = @_;

	# Save two calls to the comparison functions if $other is a Profile
	$other = $other->starting_time() if defined blessed($other) and blessed($other) eq 'Profile';

	return $other - $self->{starting_time} if $inverted;
	return $self->{starting_time} - $other;
}

1;
