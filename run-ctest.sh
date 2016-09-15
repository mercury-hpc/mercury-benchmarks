#!/bin/bash

# mirrors the filename in the utility code
svr_addr_fname="ctest-server-addr.tmp"

# MUST BE RUN IN THE SAME DIRECTORY AS THE EXECUTABLES
p=$PWD

# optionally generate this file with the additional LD_LIBRARY_PATH entries
# you need to run
ldpath=ldpath.txt

# for some reason BMI is crashing for 32MB and higher, so use a fixed (default)
# value for now
sz=$((1<<24))

# time to wait for benchmark to complete (uncomment for unlimited waiting)
timeout_cmd="timeout 60s"

# if you don't specify mpirun -np X on the command line, then MPI_Open_port will
# fail to run. Since we're 100% on ssh here, just decorate each run with a
# 1-process execution on localhost. If this causes problems, uncomment (but you
# won't be able to run the mpi class)
mpiexec_deco="mpirun -np 1 -host localhost"

# mvapich environment variables - if not set, then mercury+mpi will not work
# correctly
export MV2_ENABLE_AFFINITY=0
export MV2_SUPPORT_DPM=1
# string version of this for remote (ssh) runs
mv2_vars="MV2_ENABLE_AFFINITY=0 MV2_SUPPORT_DPM=1"

# whether all times are being printed out or not
all_opt=

# which benchmark is being run
benchmark_nr=1

# benchmark-specific options
# bench 2-4 - time duration
benchmark_timeopt=

# bench 4 - client count
benchmark4_num_clis=1
# bench 4 - how many clients to do bulk operations
benchmark4_num_bulk_clis=0
# bench 4 - whether to do rpcs or rpc+bulks
benchmark4_rpc_mode=rpc

host_default=localhost

server_host_rdma=localhost
server_host_rpc=localhost
client_hosts=(localhost)

# output files
out_prefix=cli_send
server_err=$out_prefix-srv.err
client_out=$out_prefix.out
client_err=$out_prefix.err

while getopts ":s:an:t:b:m:d:r" opt ; do
    case $opt in
        s)
            sz=$OPTARG
            ;;
        a)
            all_opt="-a"
            ;;
        n)
            benchmark_nr=$OPTARG
            ;;
        t)
            benchmark_timeopt="-t $OPTARG"
            ;;
        b)
            benchmark4_num_bulk_clis="$OPTARG"
            ;;
        m)
            benchmark4_rpc_mode="$OPTARG"
            ;;
        d)
            server_host_rdma="$OPTARG"
            ;;
        r)
            server_host_rpc="$OPTARG"
            ;;
        \?)
            echo "Invalid option: -$OPTARG" >&2
            exit 1
            ;;
        :)
            echo "Option -$OPTARG requires an argument" >&2
            exit 1
            ;;
    esac
done

shift $((OPTIND-1))

if [[ $benchmark_nr == 4 ]] ; then
    shift
    [[ -n $1 ]] && benchmark4_num_clis=$1
    shift
    if [[ $# -gt 0 ]] ; then
        client_hosts=($@)
        if [[ ${#client_hosts[@]} != $benchmark4_num_clis ]] ; then
            echo "When specifying client hosts, must have one per client" >&2
            exit 1
        fi
    fi
else
    [[ -n $1 ]] && client_hosts=($3)
fi

# generate LD_LIBRARY_PATH for remote runs
if [[ -n $ldpath ]] ; then
    ldpath_set="LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$(cat $ldpath)"
else
    ldpath_set=
fi

# for local runs, go ahead and build LD_LIBRARY_PATH
if [[ -n $ldpath_set ]] ; then
    eval export $ldpath_set
fi

# insert escape characters for strings to be interpreted by bash later on
# (e.g., when a string will be passed to a command through ssh)
function insert_escapes () {
    echo $@ | sed -r 's/\$|\;|\:|\#|\"|\(|\)/\\&/g'
}

function run () {
    set -u
    local hosttype=$1
    local class=$2
    local protocol=$3
    local host=$4
    shift 4

    if [[ $hosttype == "server" ]] ; then
        local svr_listen_port=$1
        local svr_id=${2:-} # may be empty
    else
        if [[ $benchmark_nr == 4 ]] ; then
            local cli_id=$1
            local cli_rdma_host=$2 # should be the full hg addr string
        else
            local cli_rdma_host=$1
            local cli_rpc_host=$2
        fi
    fi

    # client mode, bench 1-3- rpc server addr
    #              bench 4 - where to run client (empty->localhost)
    #local hostb=$3
    #local id=$3    # server mode - id (may be empty)
    #local hostc=$4 # client mode - where to run client in bench 1-3
    #local cli_id=$4 # client mode - id of client in bench 4

    if [[ $benchmark_nr == 1 ]] ; then
        local opts="$all_opt"
    else
        local opts="$all_opt $benchmark_timeopt"
    fi

    local prog="$timeout_cmd $mpiexec_deco ./hg-ctest$benchmark_nr $opts $hosttype $sz"
    if [[ $benchmark_nr == 4 ]] ; then
        if [[ $hosttype == "server" ]] ; then 
            prog="$prog $benchmark4_num_clis $class+$protocol://$svr_listen_port $svr_id"
        else
            if [[ $benchmark4_num_bulk_clis -gt $cli_id ]] ; then
                local mode=bulk
            else
                local mode=$benchmark4_rpc_mode
            fi
            prog="$prog $cli_id $mode $class+$protocol $cli_rdma_host"
        fi
    else
        if [[ $hosttype == "server" ]] ; then
            prog="$prog $class+$protocol://$svr_listen_port $svr_id"
        else
            prog="$prog $class+$protocol $cli_rdma_host $cli_rpc_host"
        fi
    fi

    #ready to run
    if [[ $hosttype == client ]] ; then
        if [[ $host == "" || $host =~ localhost ]] ; then
            echo "running client: $prog" >&2
            $prog
        else
            # prevent bash from substituting variables in mpi port string
            local san_prog=$(insert_escapes $prog)
            echo "running client: ssh $host cd $p; $ldpath_set $mv2_vars $san_prog" >&2
            ssh $host "cd $p; $ldpath_set $mv2_vars $san_prog"
        fi
    else
        if [[ $host =~ "localhost" ]] ; then
            echo "running server: $prog" >&2
            $prog
        else
            local san_prog=$(insert_escapes $prog)
            echo "running server: ssh $host cd $p; $ldpath_set $mv2_vars $san_prog" >&2
            ssh $host "cd $p; $ldpath_set $mv2_vars $san_prog"
        fi
    fi
}

function runtest () {
    local class=$1
    local protocol=$2

    if [[ $class == "cci" && $protocol == "sm" ]] ; then
        local server_rpc_port="0/3344"
        local server_rdma_port="0/3345"
    elif [[ $class == "mpi" && $protocol == "dynamic" ]] ; then
        local server_rpc_port=" "
        local server_rdma_port=" "
    else
        local server_rpc_port="3344"
        local server_rdma_port="3345"
    fi

    # single server
    echo "single server benchmark, $class+$protocol" \
        | tee -a $server_err $client_err
    run server $class $protocol $server_host_rpc "$server_rpc_port" \
        >> $server_err 2>&1 &
    local pid=$!
    sleep 2
    if [[ $class == "mpi" ]] ; then
        local host="$class+$protocol://$(cat $svr_addr_fname)"
    else
        local host="$class+$(cat $svr_addr_fname)"
    fi
    run client $class $protocol ${client_hosts[0]} $host $host \
        >> $client_out 2>> $client_err \
        || return $?
    wait $pid || return $?

    echo "dual server benchmark, $class+$protocol" \
        | tee -a $server_err $client_err
    run server $class $protocol $server_host_rdma "$server_rdma_port" 0 \
        >> $server_err 2>&1 &
    local pid1=$!
    run server $class $protocol $server_host_rpc "$server_rpc_port" 1 \
        >> $server_err 2>&1 &
    local pid2=$!
    sleep 2
    if [[ $class == "mpi" ]] ; then
        local host_rdma="$class+$protocol://$(cat $svr_addr_fname-0)"
        local host_rpc="$class+$protocol://$(cat $svr_addr_fname-1)"
    else
        local host_rdma="$class+$(cat $svr_addr_fname-0)"
        local host_rpc="$class+$(cat $svr_addr_fname-1)"
    fi
    run client $class $protocol ${client_hosts[0]} $host_rdma $host_rpc \
        >> $client_out 2>> $client_err \
        || return $?
    wait $pid1 || return $?
    wait $pid2 || return $?
    rm ${svr_addr_fname} ${svr_addr_fname}-0 ${svr_addr_fname}-1
}

function runtest4 () {
    local class=$1
    local protocol=$2
    local pid_arr=()

    if [[ $class == "cci" && $protocol == "sm" ]] ; then
        local port="0/3344"
    elif [[ $class == "mpi" && $protocol == "dynamic" ]] ; then
        local port=" "
    else
        local port="3344"
    fi

    # single server
    echo "N-1 benchmark, $class+$protocol" \
        | tee -a $server_err $client_err
    run server $class $protocol $server_host_rdma "$port" \
        >> $server_err 2>&1 &
    pid_arr[0]=$!
    sleep 2

    for ((i = 0; i < $benchmark4_num_clis; i++)) ; do
        run client $class $protocol ${client_hosts[$i]} $i $class+$(cat $svr_addr_fname) \
            >> $client_out.$i 2>> $client_err.$i &
        pid_arr[$((i+1))]=$!
    done

    # wait for all the client pids first - if any fail, then kill all
    local err=0
    for ((i = 1; i < $benchmark4_num_clis; i++)) ; do
        wait ${pid_arr[i]}
        err=$?
        if [[ $err -ne 0 ]] ; then
            echo "client failure, killing..."
            kill ${pid_arr[0]}
            for ((j = $((i+1)); j < $benchmark4_num_clis; j++)) ; do
                kill ${pid_arr[j]}
            done
            break
        fi
    done
    # wait for the server
    if [[ $err -eq 0 ]] ; then
        wait ${pid_arr[0]}
        err=$?
        [[ $err -ne 0 ]] && echo "server failure..."
    fi
    rm ${svr_addr_fname}

    # wait a bit to prevent copying empty file
    sleep 2
    # if everything succeeded, put all into a single file
    if [[ $err -eq 0 ]] ; then
        echo "copying $client_out.0 to $client_out"
        cat $client_out.0 >> $client_out
        cat $client_err.0 >> $client_err
        rm $client_out.0 $client_err.0
        for ((i = 1; i < $benchmark4_num_clis; i++)) ; do
            echo "copying $client_out.$i to $client_out"
            sed '/^#/d' $client_out.$i >> $client_out
            cat $client_err.$i >> $client_err
            rm $client_out.$i $client_err.$i
        done
    fi

    return $err
}

g_test=
[[ $benchmark_nr -eq 4 ]] && g_test=runtest4 || g_test=runtest
function runall () {
    #$g_test bmi tcp   || return $?
    $g_test cci tcp   || return $?
    #$g_test cci verbs || return $?
    #$g_test cci sm    || return $?
    #$g_test mpi dynamic   || return $?
}

if [[ $benchmark_nr == 1 ]] ; then
    cat > $client_out <<EOF
# format: <class> <protocol> <separate rpc/bulk svrs> <bulk size> <repetitions>
#     time (s): rpc: isolated <call> <complete>
#                    concurrent rpc-first <call> <complete>
#                    concurrent bulk-first <call> <complete>
#               bulk: isolated <call> <complete>
#                     concurrent rpc-first <call> <complete>
#                     concurrent bulk-first <call> <complete>
EOF
elif [[ $benchmark_nr == 4 ]] ; then
    cat > $client_out <<EOF
# format: <class> <protocol> <bulk size> <bench time> <type> <id>
#     time (s): <# calls> <call avg> <complete avg>
EOF
else
    cat > $client_out <<EOF
# format: <class> <protocol> <separate rpc/bulk srvs> <bulk size> <bench time>
#     time (s): rpc:  isolated   <# calls> <call avg> <complete avg>
#                     concurrent <# calls> <call avg> <complete avg>
#               bulk: isolated   <# calls> <call avg> <complete avg>
#                     concurrent <# calls> <call avg> <complete avg>
EOF
fi

runall

err=$?
if [[ $err != 0 ]] ; then
    echo "ERROR encountered (code $err) in test, exiting..."
    exit 1
fi
