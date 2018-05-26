#!/bin/bash

#######################################
#
# Description: Script to simulate random queries
#
#
#######################################

## Global Vars
declare IMPALA_SHELL=/usr/bin/impala-shell    # Impala shell
declare DRIVER_FILE                           # Which queries to execute
declare MEASURES_FILE                         # Execution metrics
declare DATABASE                              # Database to use
declare PARALLEL_DEGREE                       # Number of threads
declare QUERY_COUNT=10                        # Total # of query executions

## Enable job control for threading
set -m # Enable Job Control

####################################################
#
#    Function: get_random_query()
# Description: Selects a random query from
#              listed in DRIVER_FILE
#
####################################################
get_random_query () {

this_driver=$1

## Get the number of active queries
lc=0
while read line
do
  if [ $(echo $line | grep -c "#") -ge 1 ]
  then
     continue
  else
     let lc=lc+1
  fi
done <$this_driver

## Which active query will we execute
let random_query=$(echo $RANDOM % $lc | bc)+1

## Now loop through the driver again, skip inactive and 
## execute the random active query
lc=0
while read line
do
  if [ $(echo $line | grep -c "#") -ge 1 ]
  then
     continue
  else
     let lc=lc+1

     # Execute this lines query if it's the radom one
     if [ $lc == $random_query ]
     then
        echo "/home/ec2-user/bmark/$line"
     fi
  fi
done <$this_driver

}


####################################################
#
#    Function: exec_impala_sql
# Description: Execution of the query
#
####################################################
exec_impala_sql () {
this_query=$1

## Execute the query and capture the timing only
for line in $( { /usr/bin/time -f "%E" $IMPALA_SHELL -d $DATABASE -o /dev/null --quiet -f $1; } 2>&1 )
do
   if [ $(echo $line | grep -c ":") -gt 0 ]; then
      echo "$this_query,$line" >> $MEASURES_FILE
   fi
done

}


####################################################
#
#    Function: driver ()
# Description: Selects a random query from
#              listed in DRIVER_FILE
#
####################################################
driver () {

## Intialize Measures File with Header
## TODO - Test if already exists to avoid overwritting
echo "QUERY,TIME" > $MEASURES_FILE

for i in `seq $QUERY_COUNT`
do
  ## THIS DOESN'T WORK - ignores query execution time
  sleep 1 # used to control how long the run will last

  # Job Control, limit threads
  while [ `jobs | wc -l` -ge $PARALLEL_DEGREE ]
  do
    sleep 1
    echo "queue is full"
  done

  this_query=$(get_random_query $DRIVER_FILE)
  echo "Starting Query $this_query"
  ## TODO - Implement THINK TIME here
  sleep 1 # This should be Randomized
  exec_impala_sql $this_query &

done

echo "waiting for all queries to finish"
# Wait for all parallel jobs to finish
while [ 1 ] 
 do fg 2> /dev/null
 [ $? == 1 ] && break
done

}

usage() { echo "Usage: $0 [-c <CONTROL FILE>] [-d <database name>] " 1>&2; exit 1; }

######################################
#
# MAIN
#
######################################

## Gather command line options
## TODO - Think Time, config file option, better error messages
while getopts ":c:d:o:p:n:" m; do
    case "${m}" in
        c)
            DRIVER_FILE="${OPTARG}"
            ;;
        d)
            DATABASE=${OPTARG}
            ;;
        o)
            MEASURES_FILE=${OPTARG}
            ;;
        p)
            PARALLEL_DEGREE=${OPTARG}
            ;;
        n)
            QUERY_COUNT=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

driver
