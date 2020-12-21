# -*- coding:utf-8 -*-

import time
import os
import logging
import functools
from ssh_input import ssh_input,ssh_input_noprint,ssh_ftp
from mysql_conn import get_all, get_dict,run_noprint,more_sql
from prettytable import PrettyTable


# logging
logging.basicConfig(format="%(levelname)s\t%(asctime)s\t%(message)s",filename="mysql_rep.log")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Decorator
def log(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.debug('\ncall %s():' % func.__name__)
        return func(*args, **kwargs)
    return wrapper


# modify tht config file for mysql

@log
def modify_cnf(ori_text, modify_text):
    if len(modify_text) == 4:
        res_text = [i for i in ori_text if  'gtid' not in i.lower() and 'log-slave-updates' not in i]
    else:
        res_text = ori_text
    index = res_text.index('[mysqld]\n')
    for i in modify_text:
        res_text.insert(index+1,i)
    return res_text

# create the user for backup on master db
@log
def create_rep_user(master_db_args, slave_db_args):
    ip = slave_db_args[0]
    port = str(slave_db_args[2])
    #username = 'mcbak_' + ip.split('.')[-1] + '_' +port
    username = 'np_rep'
    m_version = get_all(master_db_args, "select version();")[0][0]

    rds_version = get_all(master_db_args,"show variables like 'rds_version';")
    if m_version[0] == '8':
        create_user_sql = f"create user '{username}'@'{ip}'  identified  with  mysql_native_password by 'npbackup';"
        grant_sql = f"grant REPLICATION SLAVE ON *.* TO '{username}'@'{ip}';"
    else:
        if rds_version==[]:
            create_user_sql = f"create user '{username}'@'{ip}' identified by 'npbackup';"
            grant_sql = f"grant REPLICATION SLAVE ON *.* TO '{username}'@'{ip}';"
        else:
            create_user_sql = f"create user '{username}'@'%' identified by 'npbackup';"
            grant_sql = f"grant REPLICATION SLAVE ON *.* TO '{username}'@'%';"

    flush_pri_sql = "flush privileges;"
    create_res = run_noprint(master_db_args, create_user_sql)
    grant_res = get_all(master_db_args, grant_sql)
    flush_res = get_all(master_db_args, flush_pri_sql)
    if create_res == [] and grant_res == [] and flush_res == []:
        print("INFO:\ncreate user for backup complete.")
        return 'create user s'
    else:
        
        return 'create user f'

# start the replicate on slave db

@log
def start_slave(master_db_args, slave_db_args,slave_os_args, mode):
    ip = slave_db_args[0]
    port = str(slave_db_args[2])
    # username = 'mcbak_' + ip.split('.')[-1] + '_' +port
    username = 'np_rep'
    if 'normal' in mode:
        change_pos_sql = get_pos(slave_os_args,slave_db_args)
        print(change_pos_sql,change_pos_sql[35:])
        if change_pos_sql == 'file not access':
            master_status = get_all(master_db_args, "show master status;")[0]
            master_log_file, master_log_pos = master_status[0], master_status[1]
            slave_config_sql = f"change master to master_host='{master_db_args[0]}',master_user='{username}',master_password='npbackup',master_port={master_db_args[2]},master_log_file='{master_log_file}',master_log_pos={master_log_pos};"
        else:
            slave_config_sql = f"change master to master_host='{master_db_args[0]}',master_user='{username}',master_password='npbackup',master_port={master_db_args[2]},master_log_file{change_pos_sql[35:]}"
    elif 'gtid' in mode:
        slave_config_sql = f"change master to master_host='{master_db_args[0]}',master_user='{username}',master_password='npbackup',master_port={master_db_args[2]},MASTER_AUTO_POSITION = 1;"
        run_noprint(slave_db_args,"change master to master_auto_position=0;")

    print(slave_config_sql)
    run_noprint(slave_db_args, "reset slave;")
    run_noprint(slave_db_args, slave_config_sql)
    run_noprint(slave_db_args, "start slave;")
    time.sleep(5)
    slave_rep_status = get_all(slave_db_args, "show slave status;")[0]

    slave_io_running = slave_rep_status[10]
    slave_sql_running = slave_rep_status[11]

    print(f" Slave_IO_Running:{slave_io_running}\n",
          f"Slave_SQL_Running:{slave_sql_running}\n")

    # start slave sync

    master_start_semi_sql = "install plugin rpl_semi_sync_master soname 'semisync_master.so';"
    master_set_semi_sql = "set global rpl_semi_sync_master_enabled=on;"
    slave_start_semi_sql = "install plugin rpl_semi_sync_slave soname 'semisync_slave.so';"
    slave_set_semi_sql = "set global rpl_semi_sync_slave_enabled=on;"
    time.sleep(5)
    if 'Yes' in slave_io_running and 'Yes' in slave_sql_running:
        if 'semi' in mode:
            run_noprint(master_db_args, master_start_semi_sql)
            run_noprint(master_db_args, master_set_semi_sql)
            run_noprint(slave_db_args, slave_start_semi_sql)
            run_noprint(slave_db_args, slave_set_semi_sql)

            run_noprint(slave_db_args, "stop slave io_thread;")
            run_noprint(slave_db_args, "start slave io_thread;")
            time.sleep(5)
            master_semi_res = get_all(
                master_db_args, "show global status like '%semi%';")
            master_semi_res = [':'.join(i) for i in master_semi_res]
            master_semi_status = '\n'.join(master_semi_res)
            print(
                f"INFO:\nMaster Semi sync Status:\n----------------------\n{master_semi_status}")

            slave_semi_res = get_all(
                slave_db_args, "show global status like '%semi%';")
            slave_semi_res = [':'.join(i) for i in slave_semi_res]
            slave_semi_status = '\n'.join(slave_semi_res)
            print(
                f"INFO:\nSlave Semi Sync Status:\n----------------------\n{slave_semi_status}\n")

        elif 'async' in mode:
            pass
        print(f"===============INFO:IP :{ip} PORT:{port}START SLAVE COMPLETE====================")
        return 'start slave complete.'
    else:
        return 'start slave failed.'

# check database status

@log
def rep_status(master_db_args, slave_db_args):
    print(F"+++++++++++++++++++++++++++++++++++\n## CHECK THE SLAVE WITH IP :{slave_db_args[0]} PORT:{slave_db_args[2]}\n")
    m_server_id = get_all(master_db_args,"show variables like 'server_id';")[0][1]
    s_server_id = get_all(slave_db_args,"show variables like 'server_id';")[0][1]
    m_version = get_all(master_db_args, "select version();")[0][0]
    s_version = get_all(slave_db_args, "select version();")[0][0]
    m_binlog = get_all(master_db_args,"show variables like 'log_bin';")[0][1]
    s_binlog = get_all(slave_db_args,"show variables like 'log_bin';")[0][1]
    m_binlog_format = get_all(master_db_args,"show variables like 'binlog_format';")[0][1]
    s_binlog_format = get_all(slave_db_args,"show variables like 'binlog_format';")[0][1]
    
    check_master_res = get_dict(master_db_args, "show master status;")
    check_slave_res = get_dict(slave_db_args, "show slave status;")



    print("###Check the status for init:")
    a= PrettyTable(['Parameter name','Master','Slave','Status'])
    a.align['Parameter name'] = "1"
    a.padding_width = 10
    if m_server_id!=s_server_id and m_server_id!='' and s_server_id!='':
        a.add_row(['server id',m_server_id,s_server_id,'passed'])
    else:
        a.add_row(['server id',m_server_id,s_server_id,'failed'])
    if m_version[0:3] == s_version[0:3]:
        a.add_row(['version',m_version,s_version,'passed'])
    else:
        a.add_row(['version',m_version,s_version,'failed'])
    if m_binlog == 'ON' and s_binlog == 'ON':
        a.add_row(['bin log',m_binlog,s_binlog,'passed'])
    else:
        a.add_row(['bin log',m_binlog,s_binlog,'failed'])
    if m_binlog_format == 'ROW' and s_binlog_format == 'ROW':
        a.add_row(['bin log format',m_binlog_format,s_binlog_format,'passed'])
    else:
        a.add_row(['bin log format',m_binlog_format,s_binlog_format,'failed'])
    print(a)

    print("\n###Check the status for MASTER:")
    b = PrettyTable(['Parameter name','Value'])
    b.align['Parameter name'] = "1"
    b.padding_width = 10
    if check_master_res != []:
        for key,value in check_master_res[0].items():
            b.add_row([key,value])
    else:
        b.add_row(['status','NO'])
    print (b)

    print("\n###Check the status for SLAVE:")
    c = PrettyTable(['Parameter name','Value'])
    c.align['Parameter name'] = "1"
    c.padding_width = 10
    if check_slave_res != []:
        for key,value in check_slave_res[0].items():
            c.add_row([key,value])
    else:
        c.add_row(['status','NO'])
    print (c)


    
    return 0


# reset master and slave
@log
def reset_slave(master_db_args, slave_db_args,master_cnf_dir,master_os_args,slave_os_args,slave_cnf_dir):


    rds_version = get_all(master_db_args,"show variables like 'rds_version';")
    if rds_version ==[]:
        check_mas_cnf_exsits = ssh_input_noprint(master_os_args,f"ls {master_cnf_dir}_ori.bak")[0]
        if " cannot access " not in check_mas_cnf_exsits :
            ssh_input(master_os_args,
              f"mv {master_cnf_dir}_ori.bak {master_cnf_dir}")
    else:
        pass
    check_sla_cnf_exsits = ssh_input_noprint(slave_os_args,f"ls {slave_cnf_dir}_ori.bak")[0]
    if " cannot access " not in check_sla_cnf_exsits :
        ssh_input(slave_os_args,
              f"mv {slave_cnf_dir}_ori.bak {slave_cnf_dir}")
    set_readonly_master(master_db_args,'off',master_cnf_dir,master_os_args)
    set_readonly_master(slave_db_args,'off',slave_cnf_dir,slave_os_args)
 
    select_drop_sql = '''select concat("drop user '",user,"'@'",host,"';") from mysql.user where user like 'np_rep%' ;'''
    select_drop_res_m = get_all(master_db_args,select_drop_sql)
    for sql in select_drop_res_m:
        run_noprint(master_db_args,sql[0])
    select_drop_res_s = get_all(slave_db_args,select_drop_sql)
    for sql_s in select_drop_res_s:
        run_noprint(slave_db_args,sql_s[0])
    set_con_warn = "SET @@GLOBAL.ENFORCE_GTID_CONSISTENCY = WARN;"
    set_con_off = "SET @@GLOBAL.ENFORCE_GTID_CONSISTENCY = OFF;"
    set_mode_off_p = "SET @@GLOBAL.GTID_MODE = OFF_PERMISSIVE;"
    set_mode_on_p = "SET @@GLOBAL.GTID_MODE = ON_PERMISSIVE;"
    set_mode_off = "SET @@GLOBAL.GTID_MODE = OFF;"


    
    run_noprint(master_db_args,"set global rpl_semi_sync_master_enabled=off;")
    run_noprint(slave_db_args,"set global rpl_semi_sync_slave_enabled=off;")
    run_noprint(master_db_args, "reset master;")
    run_noprint(slave_db_args, "stop slave;")

    get_dict(master_db_args,set_mode_on_p)
    get_dict(master_db_args,set_mode_off_p)
    get_dict(master_db_args,set_mode_off)
    get_dict(master_db_args,set_con_warn)
    get_dict(master_db_args,set_con_off)
    get_dict(slave_db_args,set_mode_on_p)
    get_dict(slave_db_args,set_mode_off_p)
    get_dict(slave_db_args,set_mode_off)
    get_dict(slave_db_args,set_con_warn)
    get_dict(slave_db_args,set_con_off)

    run_noprint(slave_db_args, "reset slave all;")
    run_noprint(slave_db_args, "reset master;")
    rep_status(master_db_args, slave_db_args)
    print("===============INFO:RESET COMPLETE====================")
    return "reset complete"


# mysqldump the database to slave database
@log
def mysqldump(master_db_args, slave_db_args,master_os_args,slave_os_args,xbakup_dir):
    m_ip, m_db_user, m_db_port, m_db_pwd = master_db_args
    s_ip, s_db_user, s_db_port, s_db_pwd = slave_db_args
    m_db_pwd = m_db_pwd.replace('!','\\!')
    s_db_pwd = s_db_pwd.replace('!','\\!')
    slave_base_dir = get_all(slave_db_args," show variables like 'basedir';")[0][1]
    print(f"INFO:\nIP:{s_ip} PORT:{s_db_port} MYSQLDUMP NOW.\n")
    exp_dir = xbakup_dir
    check_exp_dir = ssh_input_noprint(slave_os_args,f"ls {exp_dir}")
    if check_exp_dir != [] and 'no ac' in check_exp_dir[0]:
        ssh_input_noprint(slave_os_args,f"sudo mkdir -p {exp_dir}\nsudo chmod -R 777 {exp_dir}")
        print(f"INFO:\nmkdir the directory :{exp_dir} now.\n")
    elif exp_dir == '/':
        print("PLEASE DONT CHOOSE THE '/' DIRECTORY!\n")
        os._exit(0)
    else:
        ssh_input_noprint(slave_os_args,f"sudo chmod -R 777 {exp_dir}")
    ssh_input_noprint(slave_os_args,f"rm -rf {exp_dir}/exp_{s_ip}_{s_db_port}.txt")
    exp_dbs_yn = 'Y'
    
    if exp_dbs_yn.upper() == 'Y':
        exp_dbs = ssh_input(slave_os_args,f"{slave_base_dir}/bin/mysql -e 'show databases;' -u{m_db_user} -p{m_db_pwd} -P{m_db_port} -h{m_ip}| \
            grep -Ev 'Database|information_schema|mysql|performance_schema|sys' |xargs")
        if exp_dbs !=[]:
            exp_dbs = exp_dbs[0].replace('\n','')
            exp_cmd = f"{slave_base_dir}/bin/mysqldump -u{m_db_user} -p{m_db_pwd} -P{m_db_port} -h{m_ip} \
            --databases {exp_dbs} --single-transaction --master-data=2  -E -R  > {exp_dir}/exp_{s_ip}_{s_db_port}.txt"
        else:
            print("INFO:There are no business databases.")
            os._exit(0)
    else:
        exp_cmd = f"{slave_base_dir}/bin/mysqldump -u{m_db_user} -p{m_db_pwd} -P{m_db_port} -h{m_ip} \
            --single-transaction --master-data=2  -E -R --all-databases > {exp_dir}/exp_{s_ip}_{s_db_port}.txt"
    imp_cmd = f"{slave_base_dir}/bin/mysql -u{s_db_user} -p{s_db_pwd} -P{s_db_port} -h{s_ip}  <{exp_dir}/exp_{s_ip}_{s_db_port}.txt"
    exp_res = ''.join(ssh_input(slave_os_args,exp_cmd))
    if 'ERROR' not in exp_res.upper():
        exp_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print(f"INFO:\n{exp_time} IP:{s_ip} PORT:{s_db_port} EXPORT DATABASE COMPLETE.\n")
        run_noprint(slave_db_args,"reset master;")
        imp_res = ''.join(ssh_input(slave_os_args,imp_cmd))
        imp_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        if 'ERROR' not in imp_res.upper() and 'bash:' not in imp_res:
            print(f"INFO:\n{imp_time} IP:{s_ip} PORT:{s_db_port} IMPORT DATABASE COMPLETE.\n")
            print(f"===============INFO:MYSQLDUMP IP:{s_ip} PORT:{s_db_port}  COMPLETE====================")
            ssh_input_noprint(slave_os_args,f"rm -fr /tmp/{s_ip}_{s_db_port}.log\necho '{exp_dir}/exp_{s_ip}_{s_db_port}.txt'>/tmp/{s_ip}_{s_db_port}.log")
            return "imp complete"
        else:
            print(f"ERROR:\n{imp_time} IP:{s_ip} PORT:{s_db_port} IMPORT  DATABASE FAILED.\n")
            ssh_input_noprint(slave_os_args,f"rm -fr /tmp/{s_ip}_{s_db_port}.log\necho 'imp failed'>/tmp/{s_ip}_{s_db_port}.log")
            return "imp failed"
    else:
        print(f"ERROR:\nIP:{s_ip} PORT:{s_db_port} EXPORT DATABASE FAILED.\n")
        ssh_input_noprint(slave_os_args,f"rm -fr /tmp/{s_ip}_{s_db_port}.log\necho 'exp failed'>/tmp/{s_ip}_{s_db_port}.log")
        return "exp failed"


# get master position from dump file
@log
def get_pos(slave_os_args,slave_db_args):
    s_ip, s_db_user, s_db_port, s_db_pwd = slave_db_args
    dump_file_dir = ssh_input_noprint(slave_os_args,f'cat /tmp/{s_ip}_{s_db_port}.log')[0]
    if 'No such' not in dump_file_dir and '没有' not in dump_file_dir:
        if 'exp_' in dump_file_dir:
            change_pos_sql = ssh_input(slave_os_args,f'''grep "CHANGE MASTER TO MASTER_LOG_FILE='" {dump_file_dir}''')[0]
        else:
            pos_info = dump_file_dir.replace('\n','').split(' ')
            change_pos_sql = f"-- CHANGE MASTER TO MASTER_LOG_FILE='{pos_info[0]}', MASTER_LOG_POS={pos_info[-1]};"
        ssh_input_noprint(slave_os_args,f'rm -rf  /tmp/{s_ip}_{s_db_port}.log')
        return change_pos_sql
    else:
        return "file not access"


# set read_only in 5.5/5.6;set read_only,super_read_only in 5.7/8.0
@log
def set_readonly(slave_cnf_dir,slave_os_args,slave_db_args):
    s_ori_text = ssh_input(slave_os_args, f"cat {slave_cnf_dir}")
    s_version = get_all(slave_db_args, "select version();")[0][0]
    if s_version[0:3] in ['5.5','5.6']:
        modify_text = ['read_only=on\n']
        get_all(slave_db_args, "set global read_only=on;")
    else:
        modify_text = ['read_only=on\n','super_read_only=on\n']
        get_all(slave_db_args, "set global read_only=on;")
        get_all(slave_db_args, "set global super_read_only=on;")
    slave_cnf_text = ''.join(modify_cnf(s_ori_text, modify_text))

    ssh_input(slave_os_args,
        f'''mv {slave_cnf_dir} {slave_cnf_dir}_readonly.bak\necho """{slave_cnf_text}""">>{slave_cnf_dir}''')
    return "set readonly complete."

# check the status and parameter before switch
@log
def check_switch(master_db_args, slaves):

    check_master_res = get_dict(master_db_args, "show master status;")
    

    
    print(f"\n###Check the parameter for MASTER IP:{master_db_args[0]}")
    info_master = []
    b = PrettyTable(['Parameter name','Value'])
    b.align['Parameter name'] = "1"
    b.padding_width = 10
    if check_master_res != []:
        for key,value in check_master_res[0].items():
            if 'Binlog' in key:
                b.add_row([key,value])
                if value != '':
                    info_master.append(f'WARNING:The parameter <{key}> not null, maybe will make data inconsistency after switch!\n')
    else:
        b.add_row(['status','NO'])
    print (b)
    print (''.join(info_master))


    d = PrettyTable(['Serial_Number','IP','PORT','Slave_IO_Running','Slave_SQL_Running','Seconds_Behind_Master','Advice'])
    d.align['IP'] = "1"
    d.padding_width = 5
    for num,slave in enumerate(slaves):
        slave_db_args = slave[0]
        num += 1
        check_slave_res = get_dict(slave_db_args, "show slave status;")
        print(f"\n###Check the parameter for SLAVE IP:{slave_db_args[0]} PORT:{slave_db_args[2]}")
        c = PrettyTable(['Parameter name','Value'])
        c.align['Parameter name'] = "1"
        c.padding_width = 10
        info_slave = []
        if check_slave_res != []:
            for key,value in check_slave_res[0].items() :
                if 'Replicate_' in key and '_Server' not in key and 'Rewrite_' not in key:
                    c.add_row([key,value])
                    if value != '':
                        info_slave.append(f'WARNING:The parameter <{key}> not null, maybe will make data inconsistency after switch!\n')
            advice = 'pass'
            if check_slave_res[0]['Slave_IO_Running'] != 'Yes' or check_slave_res[0]['Slave_SQL_Running'] != 'Yes':
                advice = 'WARNING:The master-slave exception needs to be fixed before switch.'
            elif int(check_slave_res[0]['Seconds_Behind_Master']) <= 600 and int(check_slave_res[0]['Seconds_Behind_Master'])!=0:
                advice = 'INFO:The master-slave is normal with little delay.'
            elif int(check_slave_res[0]['Seconds_Behind_Master']) > 600:
                advice = 'WARNING:The master-slave is normal with large delay.'
            elif int(check_slave_res[0]['Seconds_Behind_Master']) ==0:
                advice = 'INFO:The master-slave is normal with no delay.'

            d.add_row([num,slave_db_args[0],slave_db_args[2],check_slave_res[0]['Slave_IO_Running'],
                check_slave_res[0]['Slave_SQL_Running'],check_slave_res[0]['Seconds_Behind_Master'],advice])
        else:
            c.add_row(['status','NO'])
            d.add_row([num,slave_db_args[0],slave_db_args[2],'None','None','None','WARNING:There are no master-slave relationship.'])
        print (c)
        print (''.join(info_slave))
    
    print("\n###Check the status for switch:")
    print (d)
    return d.get_string()


# set read only on/off  at master,mode = on or off
@log
def set_readonly_master(mysql_db_args,mode,mysql_cnf_dir,mysql_os_args):
    ori_text = ssh_input(mysql_os_args, f"cat {mysql_cnf_dir}")
    m_version = get_all(mysql_db_args, "select version();")[0][0]
    if m_version[0:3] in ['5.5','5.6']:
        sqls = ['flush tables with read lock;',f'set global read_only={mode};','flush logs;','unlock tables;']
        modify_text = [f'read_only={mode}\n']
    else:
        sqls = ['flush tables with read lock;',f'set global read_only={mode};',f'set global super_read_only={mode};','flush logs;','unlock tables;']
        modify_text = [f'read_only={mode}\n',f'super_read_only={mode}\n']
    more_sql(mysql_db_args,sqls)
    ori_text = [i for i in ori_text if  'read_only' not in i.lower() ]
    mysql_cnf_text = ''.join(modify_cnf(ori_text, modify_text))
    ssh_input(mysql_os_args,
        f'''mv {mysql_cnf_dir} {mysql_cnf_dir}_reset\necho """{mysql_cnf_text}""">>{mysql_cnf_dir}''')
    return "set readonly complete."

# check position status  before switch
@log
def check_pos(master_db_args, slaves):

    check_master_res = get_dict(master_db_args, "show master status;")[0]
    m_file = check_master_res['File']
    m_pos = check_master_res['Position']
    print(f"\n###Check the position for MASTER IP:{master_db_args[0]}")
    e = PrettyTable(['File','Position'])
    e.align['Parameter name'] = "1"
    e.padding_width = 10
    e.add_row([m_file,m_pos])
    print(e)

    print("\n###Check the status for switch:")
    f = PrettyTable(['Serial_Number','IP','PORT','Master_Log_File','Read_Master_Log_Pos','Exec_Master_Log_Pos','Advice'])
    f.align['IP'] = "1"
    f.padding_width = 5
    for num,slave in  enumerate(slaves):
        slave_db_args = slave[0]
        num += 1
        check_slave_res = get_dict(slave_db_args, "show slave status;")[0]
        s_file = check_slave_res['Master_Log_File']
        s_read_pos = check_slave_res['Read_Master_Log_Pos']
        s_exec_pos = check_slave_res['Exec_Master_Log_Pos']
        if s_file == m_file and m_pos == s_read_pos and m_pos==s_exec_pos:
            f.add_row([num,slave_db_args[0],slave_db_args[2],s_file,s_read_pos,s_exec_pos,'synchronized'])
            
        else:
            f.add_row([num,slave_db_args[0],slave_db_args[2],s_file,s_read_pos,s_exec_pos,'Unsynchronized'])

    print(f)
    return f.get_string()

# mkdir a dir for mount nfs and put backup files
@log
def mount_nfs(xbakup_dir,slaves,master_os_args):
    dir_counts = len(xbakup_dir.split('/'))
    if dir_counts >2 :
        print("INFO:The directory of backup are  available.\n")
        ssh_input_noprint(master_os_args,f'sudo mkdir -p {xbakup_dir}\nsudo chmod 777 {xbakup_dir}')
        check_soft = ''.join(ssh_input(master_os_args,f"sudo ls {xbakup_dir}"))
        if 'percona-xtrabackup' in check_soft:
            print("INFO:There have xtrabackup software in directory.")
        else:
            ssh_ftp(master_os_args,f'{xbakup_dir}/percona-xtrabackup-2.4.13.tar.gz','percona-xtrabackup-2.4.13.tar.gz','put')
            ssh_input_noprint(master_os_args,f'sudo tar -zxvf {xbakup_dir}/percona-xtrabackup-2.4.13.tar.gz -C {xbakup_dir}')
        exports_text = ssh_input(master_os_args,'sudo cat /etc/exports')
        for slave in slaves:
            slave_db_args, slave_os_args, slave_cnf_dir = slave
            if xbakup_dir not in ''.join(exports_text) and slave_os_args[0]!=master_os_args[0]:
                exports_text.append(f'{xbakup_dir}  {slave_db_args[0]}(rw,sync,all_squash)')
            ssh_input_noprint(slave_os_args,f'sudo mkdir -p {xbakup_dir}\nsudo chmod 777 {xbakup_dir}')
        ssh_input_noprint(master_os_args,f'''sudo mv  /etc/exports  /etc/exports.bak\nsudo sh -c "echo '{''.join(exports_text)}' >>  /etc/exports"''')
        rhel_version = ssh_input(master_os_args,'uname -r')[0]
        if 'el7' in rhel_version:
            ssh_input_noprint(master_os_args,'sudo exportfs -r\nsudo systemctl stop nfs\nsudo rpcbind stop\nsudo rpcbind reload\nsudo systemctl start nfs')
        else:
            ssh_input_noprint(master_os_args,'sudo exportfs -r\nsudo nfs stop\nsudo rpcbind stop\nsudo rpcbind reload\nsudo nfs start')
        check_nfs = ssh_input(master_os_args,'sudo showmount -e localhost')
        
        for slave in slaves:
            slave_db_args, slave_os_args, slave_cnf_dir = slave
            if slave_os_args[0]!=master_os_args[0]:
                mount_res = ''.join(ssh_input(slave_os_args,f'sudo mount -t nfs {master_os_args[0]}:{xbakup_dir} {xbakup_dir}  -o proto=tcp -o nolock'))
                if 'already mounted' in mount_res or mount_res=='':
            
                    print(f"INFO: IP:{slave_db_args[0]} already mounted.")
                    print("===============INFO:NFS MOUNTED COMPLETE====================")
                    return "nfs mount complete"
                else:
                    print(f"WARNING: IP:{slave_db_args[0]} were mount failed.")
                    return "nfs mount failed"
        

    else:
        print("ERROR:Please choose a Two level directory!\n")
        return "invaild dir"


# backup database by innobackupex 
@log
def innobackup(master_os_args,master_db_args,master_cnf_dir,xbakup_dir,slaves):
    mount_res = mount_nfs(xbakup_dir,slaves,master_os_args)
    if mount_res != 'nfs mount complete':
        os._exit(0)
    else:
        
        m_ip, m_db_user, m_db_port, m_db_pwd = master_db_args
        m_db_pwd = m_db_pwd.replace('!','\\!')
        m_socket_dir = get_all(master_db_args,"show global variables like 'socket'")[0][1]
        master_bakup_cmd = f"{xbakup_dir}/percona-xtrabackup-2.4.13-Linux-x86_64/bin/innobackupex --defaults-file={master_cnf_dir} \
            -u{m_db_user} -p{m_db_pwd}  --socket={m_socket_dir} --slave-info {xbakup_dir}"
        ssh_input_noprint(master_os_args,f"sudo rm -rf {xbakup_dir}/20*")
        res = ''.join(ssh_input_noprint(master_os_args,master_bakup_cmd))
    # ssh_input_noprint(master_os_args,f"chmod -R 777 {xbakup_dir}/20*")
    
        if ' completed OK!' in res:
            print("INFO:master db innobackup completed.\n")
            print("===============INFO:INNOBACKUP MASTER  COMPLETE====================")
            # for slave in slaves:
            #     slave_db_args, slave_os_args, slave_cnf_dir = slave
            #     check_dir_res = ssh_input_noprint(slave_os_args,f'cat {xbakup_dir}/{slave_db_args[0]}_{slave_db_args[2]}')[0]
            #     if  'No such' not in check_dir_res and '没有' not in check_dir_res:
            #         pass
            #     else:
            #         data_dir = get_all(slave_db_args,"show variables like 'datadir'")[0][1]
            #         if data_dir != check_dir_res:
            #             ssh_input_noprint(master_os_args,f"rm -rf {xbakup_dir}/{slave_db_args[0]}_{slave_db_args[2]}\necho '{data_dir}'>>{xbakup_dir}/{slave_db_args[0]}_{slave_db_args[2]}")

            apply_res = ''.join(ssh_input_noprint(master_os_args,f"{xbakup_dir}/percona-xtrabackup-2.4.13-Linux-x86_64/bin/innobackupex   --apply-log {xbakup_dir}/20*"))
            ssh_input_noprint(master_os_args,f"sudo chmod -R 777 {xbakup_dir}/20*")
            for slave in slaves:
                slave_db_args, slave_os_args, slave_cnf_dir = slave
                print(F"###INFO:IP:{slave_db_args[0]} PORT:{slave_db_args[2]} Innobackup start:\n")

            #   data_dir = ''.join(ssh_input_noprint(slave_os_args,f"cat {xbakup_dir}/{slave_db_args[0]}_{slave_db_args[2]}")).replace('\n','')
                #base_dir = [i for i in ssh_input_noprint(slave_os_args,f"cat {slave_cnf_dir} |grep -i basedir") if '#' not in i][0].replace('\n','').replace(' ','').split('=')[-1]
                #data_dir = [i for i in ssh_input_noprint(slave_os_args,f"cat {slave_cnf_dir} |grep -i datadir") if '#' not in i][0].replace('\n','').replace(' ','').split('=')[-1]
                base_dir = get_all(slave_db_args," show variables like 'basedir';")[0][1]
                data_dir = get_all(slave_db_args," show variables like 'datadir';")[0][1]
                redo_dir = get_all(slave_db_args," show variables like 'innodb_log_group_home_dir';")[0][1]
                undo_dir = get_all(slave_db_args," show variables like 'innodb_undo_directory';")[0][1]
                db_dir = '/'.join((data_dir+'/').replace('//','/').split('/')[0:-2])
                #ssh_input_noprint(slave_os_args,"ps -aux | grep mysqld| awk '{print $2}' | xargs kill")
                #ssh_input_noprint(slave_os_args,f"mysqladmin -u{slave_db_args[1]} -p{slave_db_args[3]} -h{slave_db_args[0]} -P{slave_db_args[2]} shutdown ")
                stop_res,bin_dir = stop_db(slave_os_args,slave_db_args)
                ssh_input_noprint(slave_os_args,f"sudo rm -rf {xbakup_dir}/data_dir_{slave_db_args[0]}_{slave_db_args[2]}")
                ssh_input_noprint(slave_os_args,f"sudo mkdir -p  {xbakup_dir}/data_dir_{slave_db_args[0]}_{slave_db_args[2]}\n\
                    sudo chmod -R 777 {xbakup_dir}/data_dir_{slave_db_args[0]}_{slave_db_args[2]}")
                ssh_input_noprint(slave_os_args,f"sudo chmod -R 777 {db_dir}")
                ssh_input_noprint(slave_os_args,f"sudo mv   {data_dir} {xbakup_dir}/data_dir_{slave_db_args[0]}_{slave_db_args[2]}")
                ssh_input_noprint(slave_os_args,f"sudo mv   {redo_dir} {xbakup_dir}/data_dir_{slave_db_args[0]}_{slave_db_args[2]}")
                ssh_input_noprint(slave_os_args,f"sudo mv   {undo_dir} {xbakup_dir}/data_dir_{slave_db_args[0]}_{slave_db_args[2]}")
                print("INFO:Shutdown mysqld server and run innobackupex --apply-log.\n")
                
                
                if ' completed OK!' in apply_res:
                    print("INFO:Run innobackupex --apply-log complete\n")
                    copy_res = ''.join(ssh_input_noprint(slave_os_args,f'''{xbakup_dir}/percona-xtrabackup-2.4.13-Linux-x86_64/bin/innobackupex --defaults-file={slave_cnf_dir} \
                        --copy-back {xbakup_dir}/20*'''))
                    if ' completed OK!' in copy_res:
                        print(f"INFO: copy back complete\n")
                        ssh_input_noprint(slave_os_args,f'cp {xbakup_dir}/20*/xtrabackup_binlog_info  /tmp/{slave_db_args[0]}_{slave_db_args[2]}.log')
                        start_db(slave_os_args,slave_db_args,bin_dir,slave_cnf_dir)
                        print(f"===============INFO:INNOBACKUP IP:{slave_db_args[0]} PORT:{slave_db_args[2]} COMPLETE====================")
                        
                        
                        
                    else:
                        print("INFO:Run innobackupex copy back failed\n")
                        restore_db(slave_db_args, slave_os_args, slave_cnf_dir ,xbakup_dir,data_dir,redo_dir,undo_dir,bin_dir)
                        print("INFO:Restore databse now\n")
                        

                else:
                    print("INFO:Run innobackupex --apply-log failed\n")
                    restore_db(slave_db_args, slave_os_args, slave_cnf_dir ,xbakup_dir,data_dir,redo_dir,undo_dir,bin_dir)
                    print("INFO:Restore databse now\n")
                    
            return 0

            


        else:
            print("INFO:master db innobackup falied.\n")
            return "backup failed"

# restore db when innobackuo failed
@log
def restore_db(slave_db_args, slave_os_args, slave_cnf_dir ,xbakup_dir,data_dir,redo_dir,undo_dir,bin_dir):
    ssh_input_noprint(slave_os_args,f"sudo rm -rf {data_dir}")
    ssh_input_noprint(slave_os_args,f"sudo rm -rf {redo_dir}")
    ssh_input_noprint(slave_os_args,f"sudo rm -rf {undo_dir}")
    db_dir = '/'.join((data_dir+'/').replace('//','/').split('/')[0:-2])
    ssh_input_noprint(slave_os_args,f"sudo chmod -R 777 {xbakup_dir}/data_dir_{slave_db_args[0]}_{slave_db_args[2]}")
    ssh_input_noprint(slave_os_args,f"sudo mv  {xbakup_dir}/data_dir_{slave_db_args[0]}_{slave_db_args[2]}/* {db_dir} ")
    start_db(slave_os_args,slave_db_args,bin_dir,slave_cnf_dir)
    return 0


    
#stop  db
@log
def stop_db(os_args,db_args):
    base_dir = get_all(db_args," show variables like 'basedir';")[0][1]
    socket_dir = get_all(db_args,"show global variables like 'socket'")[0][1]
    bin_dir = f"{base_dir}/bin/"
    ip, db_user, db_port, db_pwd = db_args
    db_pwd = db_pwd.replace('!','\\!')
    stop_res = ''.join(ssh_input_noprint(os_args,f"{bin_dir}mysqladmin -u {db_user} -p{db_pwd} -P {db_port} -S {socket_dir} shutdown"))
    if 'error' not in stop_res :
        print(f"INFO:\ndatabase IP:{ip} PORT:{db_port} shutdown complete.")
        return 'stop db s',bin_dir
    else:
        print(f"INFO:\ndatabase IP:{ip} PORT:{db_port} shutdown failed.")
        return 'stop db f',bin_dir

# start db
@log
def start_db(os_args,db_args,bin_dir,cnf_dir):
    ip, db_user, db_port, db_pwd = db_args
    start_sh = f'nohup {bin_dir}mysqld --defaults-file={cnf_dir} > /dev/null 2>&1 >>/tmp/startmysql{ip}{db_port}.log &'
    ssh_input_noprint(os_args,start_sh)
    time.sleep(3)
    start_res = ''.join(ssh_input_noprint(os_args,f'cat /tmp/startmysql{ip}{db_port}.log'))
    if 'ready for connections'  in start_res or start_res == '':
        print(f"INFO:\ndatabase IP:{ip} PORT:{db_port} start complete.")
        ssh_input_noprint(os_args,f'rm -f /tmp/startmysql{ip}{db_port}.log')
        return 'start success'
    else:
        print(f"WARNING:\ndatabase IP:{ip} PORT:{db_port} start  failed.")
        ssh_input_noprint(os_args,f'rm -f /tmp/startmysql{ip}{db_port}.log')
        return 'start failed'
    
# restart db
@log
def restart_db(os_args,db_args,cnf_dir):
    stop_res,bin_dir=stop_db(os_args,db_args)
    ip, db_user, db_port, db_pwd = db_args
    if stop_res == 'stop db s':
        time.sleep(2)
        start_res = start_db(os_args,db_args,bin_dir,cnf_dir)
        if start_res == 'start success':
            print(f"INFO:\ndatabase IP:{ip} PORT:{db_port} restart complete.")
            return 'restart s'
        else:
            print(f"WARNING:\ndatabase IP:{ip} PORT:{db_port} restart  failed.")
            return 'restart f'
    else:
        return 'stop failed'

