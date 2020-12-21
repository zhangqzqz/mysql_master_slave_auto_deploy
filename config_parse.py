# -*- coding:utf-8 -*-
import os
import configparser


def get_config(item,cnf):
    config = configparser.ConfigParser()
    config.read(cnf)

    ip = config.get(item, 'ip')
    db_user = config.get(item, 'db_user')
    db_port = config.getint(item, 'db_port')
    db_pwd = config.get(item, 'db_password')
    db_cnf_dir = config.get(item, 'db_config_dir')
    xbakup_dir = config.get(item, 'xbackup_dir')


    os_user = config.get(item, 'os_user')
    os_port = config.getint(item, 'os_port')
    os_pwd = config.get(item, 'os_password')

    db_args = [ip, db_user, db_port, db_pwd]
    os_args = [ip, os_port, os_user, os_pwd]
    return db_args, os_args, db_cnf_dir, xbakup_dir


def get_slaves(cnf):
    config = configparser.ConfigParser()
    config.read(cnf)
    items = [i for i in config.sections() if 'slave' in i]
    slaves = []
    for item in items:
        ip = config.get(item, 'ip')
        db_user = config.get(item, 'db_user')
        db_port = config.getint(item, 'db_port')
        db_pwd = config.get(item, 'db_password')
        db_cnf_dir = config.get(item, 'db_config_dir')


        os_user = config.get(item, 'os_user')
        os_port = config.getint(item, 'os_port')
        os_pwd = config.get(item, 'os_password')

        db_args = [ip, db_user, db_port, db_pwd]
        os_args = [ip, os_port, os_user, os_pwd]
        slaves.append([db_args, os_args, db_cnf_dir])
    return slaves


# reset config at config.cfg after switch
def reset_config(switch_num,cnf):
    switch_num -= switch_num
    config = configparser.ConfigParser()
    config.read(cnf)
    master_section = config.items('master_config')
    slave_item = [i for i in config.sections() if 'slave' in i][switch_num]
    slave_section = config.items(slave_item)
    for option_s in slave_section:
        config.set("master_config",option_s[0],option_s[1])
    for option_m in master_section:
        config.set(slave_item,option_m[0],option_m[1])
    with open(cnf, 'w') as configfile:
        config.write(configfile)
    return 0

