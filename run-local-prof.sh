#!/bin/bash

class=$1
transport=$2
mode=$3

token_file=rdy-token.tmp

# for mvapich runs
export MV2_ENABLE_AFFINITY=0
export MV2_SUPPORT_DPM=1

# parameters for controlling runs
buf_size=4096
time_to_run=120
op_type=rpc

if [[ $class == "" ]] ; then
    echo Must provide class as arg 1 >&2
    exit 1
elif [[ $transport == "" ]] ; then
    echo Must provide transport as arg 2 >&2
    exit 1
elif [[ $mode == "" ]] ; then
    mode=all
elif [[ ! ( $mode =~ (client)|(server)|(all) ) ]] ; then
    echo Unexpected mode: want "client", "server", or "all"
fi

[[ $mode == "server" || $mode == "all" ]] && run_server=yes || run_server=no
[[ $mode == "client" || $mode == "all" ]] && run_client=yes || run_client=no

if [[ $run_server == "yes" ]] ; then
    CPUPROFILE=./pprof-srv-$class-$transport.out \
        mpirun -np 1 -host $(hostname) \
        ./hg-ctest4 server $buf_size 1 $class+$transport://$(hostname):3344 &
    [[ $mode == "server" ]] && touch $token_file
fi
# do a long run so that initialization costs don't skew the benchmark
if [[ $run_client == "yes" ]] ; then
    if [[ $mode == "client" ]] ; then
        while [[ ! -f $token_file ]] ; do sleep 1 ; done
        rm $token_file
    fi
    sleep 1
    CPUPROFILE=./pprof-cli-$class-$transport.out \
        mpirun -np 1 -host $(hostname) \
        ./hg-ctest4 -t $time_to_run client $buf_size 0 $op_type $(cat ctest1-server-addr.tmp)
    err=$?
    if [[ $err -ne 0 ]] ; then
        echo Client error - exited with code $err >&2
    fi
fi

if [[ $mode == "all" || $mode == "server" ]] ; then
    wait
    err=$?
    if [[ $err -ne 0 ]] ; then
        echo Server error - exited with code $err >&2
    fi
fi
