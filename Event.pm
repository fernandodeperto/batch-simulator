package Event;
use strict;
use warnings;

use overload '<=>' => \&three_way_comparison, '""' => \&stringification;

sub new {
	my ($class, $type, $timestamp, $payload) = @_;

	my $self = {
		type => $type,
		timestamp => $timestamp,
		payload => $payload,
	};

	bless $self, $class;
	return $self;
}

sub type {
	my ($self, $type) = @_;

	$self->{type} = $type if (defined $type);

	return $self->{type};
}

sub timestamp {
	my ($self, $timestamp) = @_;

	$self->{timestamp} = $timestamp if (defined $timestamp);

	return $self->{timestamp};
}

sub payload {
	my ($self, $payload) = @_;

	$self->{payload} = $payload if (defined $payload);

	return $self->{payload};
}

sub three_way_comparison {
	my ($self, $other, $inverted) = @_;

	return $self->{type} <=> $other->{type} if
	($self->{timestamp} == $other->{timestamp});

	return $self->{timestamp} <=> $other->{timestamp};
}

sub stringification {
	my ($self) = @_;
	return "[$self->{type}, $self->{timestamp}, ($self->{payload})]";
}

1;

