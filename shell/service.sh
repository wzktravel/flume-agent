#!/bin/bash

. /etc/rc.d/init.d/functions

BASEDIR=$(cd `dirname $0`; pwd)
cd $BASEDIR
echo $BASEDIR
PIDFILE=$BASEDIR/pids/flume.pid
CONFIGFILE=$BASEDIR/conf/flume.hdfs.conf
LOCKFILE=$BASEDIR/.lock
PIDDIR=`dirname $PIDFILE`
OPTIONS=" agent --conf conf --conf-file $CONFIGFILE --no-reload-conf --name a1 -Dflume.root.logger=INFO,console"

flume=$BASEDIR/bin/flume-ng

USER=fsdevops
GROUP=fsdevops

# Handle NUMA access to CPUs (SERVER-3574)
# This verifies the existence of numactl as well as testing that the command works
NUMACTL_ARGS="--interleave=all"
if which numactl >/dev/null 2>/dev/null && numactl $NUMACTL_ARGS ls / >/dev/null 2>/dev/null
then
    NUMACTL="numactl $NUMACTL_ARGS"
else
    NUMACTL=""
fi

start()
{
  # Make sure the default pidfile directory exists
  if [ ! -d $PIDDIR ]; then
    install -d -m 0755 -o $USER -g $GROUP $PIDDIR
  fi

  echo -n $"Starting flume: "
  daemon --pidfile=$PIDFILE --check $flume "$NUMACTL $flume $OPTIONS > $BASEDIR/logs/flume.log 2>&1 & echo $! > $PIDFILE"
  RETVAL=$?
  echo
  [ $RETVAL -eq 0 ] && touch $LOCKFILE
}

stop()
{
  echo -n $"Stopping flume: "
   echo "`ps aux |fgrep flume |grep -vE \"grep|tail\" | head -1 |  awk '{print $2}'`" > $PIDFILE
  killproc "$PIDFILE" $flume
  RETVAL=$?
  echo
  [ $RETVAL -eq 0 ] && rm -f $LOCKFILE
}

restart () {
    stop
    start
}

# Send TERM signal to process and wait up to 300 seconds for process to go away.
# If process is still alive after 300 seconds, send KILL signal.
# Built-in killproc() (found in /etc/init.d/functions) is on certain versions of Linux
# where it sleeps for the full $delay seconds if process does not respond fast enough to
# the initial TERM signal.
killproc()
{
  local pid_file=$1
  local procname=$2
  local -i delay=300
  local -i duration=10
  local pid=`pidofproc -p "${pid_file}" ${procname}`

  kill -TERM $pid >/dev/null 2>&1
  usleep 100000
  local -i x=0
  while [ $x -le $delay ] && checkpid $pid; do
    sleep $duration
    x=$(( $x + $duration))
  done

  kill -KILL $pid >/dev/null 2>&1
  usleep 100000

  rm -f "${pid_file}"

  checkpid $pid
  local RC=$?
  [ "$RC" -eq 0 ] && failure "${procname} shutdown" || success "${procname} shutdown"
  RC=$((! $RC))
  return $RC
}

RETVAL=0

case "$1" in
  start)
    start
    ;;
  stop)
    stop
    ;;
  restart)
    restart
    ;;
  condrestart)
    [ -f $LOCKFILE ] && restart || :
    ;;
  status)
    status $flume
    RETVAL=$?
    ;;
  version)
    $flume version
    ;;
  *)
    echo "Usage: $0 {start|stop|status|restart|condrestart|version}"
    RETVAL=1
esac

exit $RETVAL
