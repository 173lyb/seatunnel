#!/bin/bash

# 定义服务器名称
servers=("master" "slave1" "liuheng-cdh-node3" "xugu-bigdatacenter-app4")

start(){
  echo "-------------------------------------------------"
  echo "Starting SeaTunnelServer on all servers."
  for server in ${servers[@]}; do
    echo "Starting SeaTunnelServer on ${server}."
    ssh ${server} "source /etc/profile && ${SEATUNNEL_HOME}/bin/seatunnel-cluster.sh -d"
  done
}

stop(){
  echo "-------------------------------------------------"
  echo "Stopping SeaTunnelServer on all servers."
  for server in ${servers[@]}; do
    echo "Stopping SeaTunnelServer on ${server}."
    ssh ${server} "pkill -f 'org.apache.seatunnel.core.starter.seatunnel.SeaTunnelServer'"
  done
}

restart(){
  stop
  sleep 3
  start
}

case "$1" in
  "start")
    start
    ;;
  "stop")
    stop
    ;;
  "restart")
    restart
    ;;
  *)
    echo "Usage: $0 {start|stop|restart}"
    exit 1
esac
