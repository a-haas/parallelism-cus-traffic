#!/bin/sh

make
rm -rf output*
hadoop jar cus-traffic.jar CusTraffic ../../ressources/cus-traffic.csv output