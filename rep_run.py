#!/usr/bin/env python
import argparse
import functools
import json
import logging
import sys
import paramiko
from config_parse import get_config,get_slaves
from mysql_rep import mysql_rep, mysql_rep_multi
from mysql_switch import mysql_switch
from method import reset_slave,rep_status,mysqldump,check_switch,check_pos,mount_nfs,innobackup


# global

__version__ = "1.2.2"


# logging

logging.basicConfig(
    format="%(levelname)s\t%(asctime)s\t%(message)s", filename="mysql_rep.log")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# main


def main():

    parser = argparse.ArgumentParser(description="mysql replicate")
    parser.add_argument("-c","--conf",type=str,default='config.cfg',
                        help="配置文件名，默认为本目录下的'config.cfg'")
    parser.add_argument("-t","--init_db_method",type=str,default=' ',
                        help="从库数据初始化方式，\
                            可选：|mysqldump：使用mysqldump初始化从库 |innobackup: 物理备份初始化从库,默认为空")
    parser.add_argument("-m","--mode",choices=[
                        'semi_normal', 'rep_status', 'semi_gtid','semi_gtid_rds', 'reset_slave', 'async_gtid', 'async_normal','mysqldump',
                        'check_switch','check_pos','mysql_switch','mount_nfs','innobackup'],
                        help='semi_normal:半同步传统模式| semi_gtid:半同步gtid模式| async_normal:异步传统模式| async_gtid:异步gtid模式| \
                            semi_gtid_rds:RDS下的半同步gtid模式| rep_status:查看主从状态| reset_slave:重置主从关系| mysqldump：使用mysqldump初始化从库\
                                | check_switch: 切换前检查参数| check_pos: 检查主从同步position')

    args = parser.parse_args()
    init_db_method = args.init_db_method
    conf = args.conf
    mode = args.mode

    master_db_args, master_os_args, master_cnf_dir ,xbakup_dir= get_config(
        'master_config',conf)
    slaves = get_slaves(conf)
    slave_db_args, slave_os_args, slave_cnf_dir = slaves[0]


    if init_db_method == 'mysqldump':
        print("---------------------INFO:slave init mode config start---------------------")
        for slave in slaves:
            slave_db_args_a, slave_os_args_a, slave_cnf_dir_a = slave
            mysqldump(master_db_args, slave_db_args_a,master_os_args,slave_os_args_a,xbakup_dir)
        print("---------------------INFO:slave init mode config end---------------------")
    elif init_db_method == 'innobackup':
        print("---------------------INFO:slave init mode config start---------------------")
        innobackup(master_os_args,master_db_args,master_cnf_dir,xbakup_dir,slaves)
        print("---------------------INFO:slave init mode config end---------------------")
    else:
        pass


    
    if mode == 'semi_normal':
        print("---------------------INFO:sync mode config start---------------------")
        mysql_rep(master_db_args, master_os_args, master_cnf_dir,
                  slave_db_args, slave_os_args, slave_cnf_dir, 'semi_normal')
        if len(slaves)!=1:
            for slave_multi in slaves[1:]:
                slave_db_args, slave_os_args, slave_cnf_dir = slave_multi
                mysql_rep_multi(master_db_args, master_os_args, master_cnf_dir,
                    slave_db_args, slave_os_args, slave_cnf_dir, 'semi_normal')
        print("---------------------INFO:sync mode config end---------------------")

    elif mode == 'semi_gtid':
        print("---------------------INFO:sync mode config start---------------------")
        mysql_rep(master_db_args, master_os_args, master_cnf_dir,
                  slave_db_args, slave_os_args, slave_cnf_dir, 'semi_gtid')
        if len(slaves)!=1:
            for slave_multi in slaves[1:]:
                slave_db_args, slave_os_args, slave_cnf_dir = slave_multi
                mysql_rep_multi(master_db_args, master_os_args, master_cnf_dir,
                    slave_db_args, slave_os_args, slave_cnf_dir, 'semi_gtid')
        print("---------------------INFO:sync mode config end---------------------")

    elif mode == 'semi_gtid_rds':
        print("---------------------INFO:sync mode config start---------------------")
        mysql_rep(master_db_args, master_os_args, master_cnf_dir,
                  slave_db_args, slave_os_args, slave_cnf_dir, 'semi_gtid_rds')
        if len(slaves)!=1:
            for slave_multi in slaves[1:]:
                slave_db_args, slave_os_args, slave_cnf_dir = slave_multi
                mysql_rep_multi(master_db_args, master_os_args, master_cnf_dir,
                    slave_db_args, slave_os_args, slave_cnf_dir, 'semi_gtid_rds')
        print("---------------------INFO:sync mode config end---------------------")

    elif mode == 'async_gtid':
        print("---------------------INFO:sync mode config start---------------------")
        slave_db_args, slave_os_args, slave_cnf_dir = slaves[0]
        mysql_rep(master_db_args, master_os_args, master_cnf_dir,
                  slave_db_args, slave_os_args, slave_cnf_dir, 'async_gtid')
        if len(slaves)!=1:
            for slave_multi in slaves[1:]:
                slave_db_args, slave_os_args, slave_cnf_dir = slave_multi
                mysql_rep_multi(master_db_args, master_os_args, master_cnf_dir,
                    slave_db_args, slave_os_args, slave_cnf_dir, 'async_gtid')
        print("---------------------INFO:sync mode config end---------------------")

    elif mode == 'async_normal':
        print("---------------------INFO:sync mode config start---------------------")
        mysql_rep(master_db_args, master_os_args, master_cnf_dir,
                  slave_db_args, slave_os_args, slave_cnf_dir, 'async_normal')
        if len(slaves)!=1:
            for slave_multi in slaves[1:]:
                slave_db_args, slave_os_args, slave_cnf_dir = slave_multi
                mysql_rep_multi(master_db_args, master_os_args, master_cnf_dir,
                    slave_db_args, slave_os_args, slave_cnf_dir, 'async_normal')
        print("---------------------INFO:sync mode config end---------------------")

    elif mode == 'rep_status':
        for slave_multi in slaves:
            slave_db_args = slave_multi[0]
            rep_status(master_db_args, slave_db_args)
    elif mode == 'reset_slave':
        for slave in slaves:
            slave_db_args, slave_os_args, slave_cnf_dir = slave
            reset_slave(master_db_args, slave_db_args,master_cnf_dir,master_os_args,slave_os_args,slave_cnf_dir)
    elif mode == 'check_switch':
        check_switch(master_db_args, slaves)
    elif mode == 'check_pos':
        check_pos(master_db_args, slaves)
    elif mode == 'mysql_switch':
        mysql_switch(master_db_args, master_os_args, master_cnf_dir, slave_db_args, slave_os_args, slave_cnf_dir,slaves,conf)
    else:
        return None
    

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(e)
        sys.exit(1)
