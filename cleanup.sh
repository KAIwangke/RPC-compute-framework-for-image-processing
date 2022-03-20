#!/bin/bash

count=0
while read p; 
do
  address=`echo $p | awk '{print $2}'`
  if [ $count == 0 ] ; then
     NODE_0_IP=`echo $p | cut --complement -f 1 -d' '`
  elif  [ $count == 1 ] ; then
     NODE_1_IP=`echo $p | cut --complement -f 1 -d' '`
  elif  [ $count == 2 ] ; then
     NODE_2_IP=`echo $p | cut --complement -f 1 -d' '`
  elif  [ $count == 3 ] ; then
     NODE_3_IP=`echo $p | cut --complement -f 1 -d' '`
  elif  [ $count == 4 ] ; then
     SERVER_IP=`echo $p | cut --complement -f 1 -d' '`
   elif [ $count == 5 ]; then
     CLIENT_IP=`echo $p | cut --complement -f 1 -d' '`
  fi
  count=`echo "scale=0;$count + 1" | bc`
done < machine.txt

count=0
while read p; do
   if [ $count == 0  ]; then
      NODE_COMMAND_0=`echo $p | cut --complement -f 1 -d' '`
   elif [ $count == 1  ]; then
      NODE_COMMAND_1=`echo $p | cut --complement -f 1 -d' '`
   elif [ $count == 2  ]; then
      NODE_COMMAND_2=`echo $p | cut --complement -f 1 -d' '`
   elif [ $count == 3  ]; then
      NODE_COMMAND_3=`echo $p | cut --complement -f 1 -d' '`
   elif [ $count == 4  ]; then
      SERVER_COMMAND=`echo $p | cut --complement -f 1 -d' '`
   elif [ $count == 5 ]; then
      CLIENT_COMMAND=`echo $p | cut --complement -f 1 -d' '`
   fi 
   count=`echo "scale=0;$count + 1" | bc`
done < commands.txt


jcount=`echo $NODE_COMMAND_0 | grep "/java" | wc -l`
if [ $jcount -gt 0 ]; then
   SERVER_COMMAND="/java"
   CLIENT_COMMAND="/java"
   NODE_COMMAND_0="/java"
   NODE_COMMAND_1="/java"
   NODE_COMMAND_2="/java"
   NODE_COMMAND_3="/java"
fi


USERID=`whoami`

echo "ssh -tt -n -f $USERID@$NODE_0_IP  'pkill -f \".*$NODE_COMMAND_0.*\"' -U $USERID" > kill_command_0.sh
echo "ssh -tt -n -f $USERID@$NODE_1_IP  'pkill -f \".*$NODE_COMMAND_1.*\"' -U $USERID" > kill_command_1.sh
echo "ssh -tt -n -f $USERID@$NODE_2_IP  'pkill -f \".*$NODE_COMMAND_2.*\"' -U $USERID" > kill_command_2.sh
echo "ssh -tt -n -f $USERID@$NODE_3_IP  'pkill -f \".*$NODE_COMMAND_3.*\"' -U $USERID" > kill_command_3.sh
echo "ssh -tt -n -f $USERID@$SERVER_IP  'pkill -f \".*$SERVER_COMMAND.*\"' -U $USERID" > kill_command_4.sh
echo "ssh -tt -n -f $USERID@$CLIENT_IP  'pkill -f \".*$CLIENT_COMMAND.*\"' -U $USERID" > kill_command_5.sh

source kill_command_0.sh
source kill_command_1.sh
source kill_command_2.sh
source kill_command_3.sh
source kill_command_4.sh
source kill_command_5.sh
