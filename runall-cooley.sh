#!/bin/bash

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)"
cd $script_dir

#script params
rt=./run-ctest.sh
s="-s $((1<<24))"
t="-t 5"
a=""
#a="-a"

#concurrency params
num_local_clients=4

if [[ ! -e $COBALT_NODEFILE ]] ; then
    echo "ERROR: couldn't find COBALT_NODEFILE" >&2
    exit 1
fi 

nodes_raw=( $(cat $COBALT_NODEFILE) )
num_nodes=${#nodes_raw[@]}
if [[ -z $num_nodes || $num_nodes -eq 0 ]] ; then
    echo "ERROR: COBALT_NODEFILE empty??" >&2
    exit 1
fi

# two cases - all are separated or none are. Means that we either have a single
# node alloc or >2 (rpc-server,bulk-server,client*)
if [[ ! ( $num_nodes -eq 1 || $num_nodes -gt 2 ) ]] ; then
    echo "ERROR: COBALT_NODEFILE must have either 1 node or >2" >&2
    exit 1
fi

# results subdirectory
[[ $num_nodes -eq 1 ]] && results_subdir=local || results_subdir=remote

# make sure we aren't squashing old results
if [[ -e results.cooley/$results_subdir ]] ; then
    echo "ERROR: results dir already exists - (re)move it!" >&2
    exit 1
fi

# create output directories
out_dir_pre=results.cooley/$results_subdir/ctest

# create node port pairs - in single node case, use 4 clients, one server
if [[ $num_nodes -eq 1 && 0 -eq 1 ]] ; then
    nodes=()
    for ((i = 0 ; i < $((num_local_clients+1)) ; i++)) ; do
        nodes[$i]=${nodes_raw[0]}:$((3344+i))
    done
else
    nodes=( $(awk '{print $1":3344"}' $COBALT_NODEFILE) )
fi

num_nodes=${#nodes[@]}

mkdir -p ${out_dir_pre}1
mkdir -p ${out_dir_pre}2
mkdir -p ${out_dir_pre}3
mkdir -p ${out_dir_pre}4/rpc.1
mkdir -p ${out_dir_pre}4/bulk.1
mkdir -p ${out_dir_pre}4/rpc.2
mkdir -p ${out_dir_pre}4/bulk.2
mkdir -p ${out_dir_pre}4/rpc.$((num_nodes-1))
mkdir -p ${out_dir_pre}4/bulk.$((num_nodes-1))

# run each test
nds="${nodes[0]} ${nodes[1]} ${nodes[2]}"
$rt -n 1 $s $nds
if [[ $? -ne 0 ]] ; then 
    echo "ERROR: test 1 failed" >&2 
    exit 1
fi
mv cli_send* ${out_dir_pre}1

$rt -n 2 $t $s $nds
if [[ $? -ne 0 ]] ; then 
    echo "ERROR: test 2 failed" >&2 
    exit 1
fi
mv cli_send* ${out_dir_pre}2

$rt -n 3 $t $s $nds
if [[ $? -ne 0 ]] ; then
    echo "ERROR: test 3 failed" >&2 
    exit 1
fi
mv cli_send* ${out_dir_pre}3

# test 4 - 1 rpc client
nds="${nodes[0]} ${nodes[1]}"
$rt -n 4 $t -b 0 $a $s $nds
if [[ $? -ne 0 ]] ; then
    echo "ERROR: test 4 (1 rpc client) failed" >&2
    exit 1
fi
mv cli_send* ${out_dir_pre}4/rpc.1

# 1 bulk client
$rt -n 4 -b 1 $a $t $s $nds
if [[ $? -ne 0 ]] ; then 
    echo "ERROR: test 4 (1 bulk client) failed" >&2
    exit 1
fi
mv cli_send* ${out_dir_pre}4/bulk.1

# short circuit if doing single client/server runs
[[ $num_nodes -le 2 ]] && exit 0

nds="${nodes[0]} ${nodes[1]} ${nodes[2]}"
# two-client runs
$rt -n 4 $t -b 0 $a $s $nds
if [[ $? -ne 0 ]] ; then
    echo "ERROR: test 4 (2 rpc clients) failed" >&2
    exit 1
fi
mv cli_send* ${out_dir_pre}4/rpc.2
$rt -n 4 -b 1 $a $t $s $nds
if [[ $? -ne 0 ]] ; then
    echo "ERROR: test 4 (1,1 rpc,bulk clients) failed" >&2
    exit 1
fi
mv cli_send* ${out_dir_pre}4/bulk.2

# short circuit if doing two client runs
[[ $num_nodes -le 3 ]] && exit 0

# test 4 - N rpc clients, 0 bulk
$rt -n 4 -b 0 $a $t $s ${nodes[@]}
if [[ $? -ne 0 ]] ; then
    echo "ERROR: test 4 ($((num_nodes-1)) rpc clients) failed" >&2
    exit 1
fi
mv cli_send* ${out_dir_pre}4/rpc.$((num_nodes-1))

# test 4 - 1 bulk, N-1 rpc clients
$rt -n 4 -b 1 $a $t $s ${nodes[@]}
if [[ $? -ne 0 ]] ; then
    echo "ERROR: test 4 ($((num_nodes-2)),1 rpc,bulk clients) failed" >&2
    exit 1
fi
mv cli_send* ${out_dir_pre}4/bulk.$((num_nodes-1))
