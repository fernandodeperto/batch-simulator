#!/bin/bash

BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export PERL5LIB=$PERL5LIB:$BASEDIR/ProcessorRange/blib/lib:$BASEDIR/ProcessorRange/blib/arch
