#!/usr/bin/env bash
# first argument user name 
# second one experiment name

# Do not forget the VPN

read -s -p "Enter ssh password : " PASSWORD_SSH;
sshpass -p $PASSWORD_SSH scp -r $1@euler.ethz.ch:/cluster/scratch/$1/$2 .