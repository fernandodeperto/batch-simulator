package Tree;
use strict;
use warnings;

# Constructors

sub new {
	my ($class, $content) = @_;

	my $self = {
		content => $content,
		children => [],
	};

	bless $self, $class;
	return $self;
}

# Public routines

sub add_child {
	my ($self, $child) = @_;

	push @{$self->{children}}, $child;

	return;

}

# Getters and setters

sub children {
	my ($self, $children) = @_;

	$self->{children} = $children if defined $children;

	return $self->{children};
}

sub content {
	my ($self, $content) = @_;

	$self->{content} = $content if defined $content;

	return $self->{content};
}

1;
