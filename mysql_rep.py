import os
from method import create_rep_user, modify_cnf, start_slave, rep_status,reset_slave,get_pos,set_readonly,restart_db
from ssh_input import ssh_input
import time
from mysql_conn import get_all, get_dict

# mysql replicate : one master -- one slave
def mysql_rep(master_db_args, master_os_args, master_cnf_dir, slave_db_args, slave_os_args, slave_cnf_dir, mode):
    rds_version = get_all(master_db_args,"show variables like 'rds_version';")
    if rds_version !=[] and 'rds' not in mode:
        print("###WARRING: the master db base in  RDS and you should choose the 'semi_gtid_rds' to configure replication.")
        os._exit(0)
    print(f"INFO:\nUser choose the {mode} method to config mysql replicate.\n")

    # check server id and binlog
    m_server_id = get_all(master_db_args,"show variables like 'server_id';")[0][1]
    s_server_id = get_all(slave_db_args,"show variables like 'server_id';")[0][1]

    m_binlog = get_all(master_db_args,"show variables like 'log_bin';")[0][1]
    s_binlog = get_all(slave_db_args,"show variables like 'log_bin';")[0][1]
    if m_server_id != s_server_id and m_server_id!='' and s_server_id!='':
        print("INFO:check the server-id completed.")
    else:
        print("ERROR:check the server-id failed")
        os._exit(0)
    
    if m_binlog == 'ON' and s_binlog == 'ON':
        print("INFO:check the binlog completed.")
    else:
        print("ERROR:check the binlog failed")
        os._exit(0)
    print("INFO:check the parameter completed.")
    print("===============INFO:CHECK PARAMETER BEFORE COMPLETE====================")
    # modify the mysql config file
    if 'rds' not in mode:
        m_ori_text = ssh_input(master_os_args, f"cat {master_cnf_dir}")
    else:
        m_ori_text = ['']
    s_ori_text = ssh_input(slave_os_args, f"cat {slave_cnf_dir}")
    if 'normal' in mode:
        modify_text = []
        m_text_list = modify_cnf(m_ori_text, modify_text)
        s_text_list = modify_cnf(s_ori_text, modify_text)
        master_cnf_text = ''.join(m_text_list)
        slave_cnf_text = ''.join(s_text_list)

        ssh_input(master_os_args,
              f'''mv {master_cnf_dir} {master_cnf_dir}_ori.bak\necho """{master_cnf_text}""">>{master_cnf_dir}''')
        ssh_input(slave_os_args,
              f'''mv {slave_cnf_dir} {slave_cnf_dir}_ori.bak\necho """{slave_cnf_text}""">>{slave_cnf_dir}''')
        pass
        print("===============INFO:MODIFY MY.CNF COMPLETE====================")
    elif 'gtid' in mode:

        m_version = get_all(master_db_args, "select version();")[0][0]
        s_version = get_all(slave_db_args, "select version();")[0][0]

        if m_version[0:3] == s_version[0:3] and m_version[0:3] != '5.5':

            if 'rds' in mode:
                modify_text = ['replicate-ignore-db=information_schema\n','replicate-ignore-db=performance_schema\n','replicate-ignore-db=mysql\n','replicate-ignore-db=sys\n','# GTID\n','gtid_mode=on\n', 'enforce_gtid_consistency=on\n', 'log-slave-updates=on\n']
            else:
                modify_text = ['gtid_mode=on\n', 'enforce_gtid_consistency=on\n', 'log-slave-updates=on\n','# GTID\n',]

            m_text_list = modify_cnf(m_ori_text, modify_text)
            s_text_list = modify_cnf(s_ori_text, modify_text)
            master_cnf_text = ''.join(m_text_list)
            slave_cnf_text = ''.join(s_text_list)
            #modify_time = time.strftime("%Y-%m-%d-%H-%M", time.localtime())
            if 'rds' not in mode:
                ssh_input(master_os_args,
                    f'''mv {master_cnf_dir} {master_cnf_dir}_ori.bak\necho """{master_cnf_text}""">>{master_cnf_dir}''')
            else:
                pass
            ssh_input(slave_os_args,
                f'''mv {slave_cnf_dir} {slave_cnf_dir}_ori.bak\necho """{slave_cnf_text}""">>{slave_cnf_dir}''')
            print("===============INFO:MODIFY MY.CNF COMPLETE====================")
            check_gtid_con_sql = "show global variables like 'enforce_gtid%';"
            check_gtid_mode_sql = "show global variables like 'gtid_mode%';"
            if m_version[0:3] == s_version[0:3] and m_version[0:3] != '5.6':
                set_con_warn = "SET @@GLOBAL.ENFORCE_GTID_CONSISTENCY = WARN;"
                set_con_on = "SET @@GLOBAL.ENFORCE_GTID_CONSISTENCY = ON;"
                set_mode_off = "SET @@GLOBAL.GTID_MODE = OFF_PERMISSIVE;"
                set_mode_on_p = "SET @@GLOBAL.GTID_MODE = ON_PERMISSIVE;"
                set_mode_on = "SET @@GLOBAL.GTID_MODE = ON;"

                rds_version = get_all(master_db_args,"show variables like 'rds_version';")
                if rds_version ==[]:
                    get_all(master_db_args, set_con_warn)
                    get_all(master_db_args, set_con_on)
                    get_all(master_db_args, set_mode_off)
                    get_all(master_db_args, set_mode_on_p)
                    get_all(master_db_args, set_mode_on)
                else:
                    pass
                get_all(slave_db_args, set_con_warn)
                get_all(slave_db_args, set_con_on)
                get_all(slave_db_args, set_mode_off)
                get_all(slave_db_args, set_mode_on_p)
                get_all(slave_db_args, set_mode_on)
                print("===============INFO:MODIFY GTID PARAMETER COMPLETE====================")
            elif s_version[0:3] == '5.6':
                restart_m_res = restart_db_res = restart_db(master_os_args,master_db_args,master_cnf_dir)
                restart_s_res = restart_db_res = restart_db(slave_os_args,slave_db_args,slave_cnf_dir)
                
                if restart_m_res == 'restart s' and restart_s_res == 'restart s':
                    check_gtid_con_res = get_all(master_db_args, check_gtid_con_sql)[0][1]
                    check_gtid_mode_res = get_all(master_db_args, check_gtid_mode_sql)[0][1]
                    check_gtid_con_res_slave = get_all(slave_db_args, check_gtid_con_sql)[0][1]
                    check_gtid_mode_res_slave = get_all(slave_db_args, check_gtid_mode_sql)[0][1]
                    check_gtid_con_res = get_all(master_db_args, check_gtid_con_sql)[0][1]
                    print(check_gtid_con_res,check_gtid_con_res_slave,check_gtid_mode_res,check_gtid_mode_res_slave)
                    if check_gtid_con_res=='ON' and check_gtid_mode_res=='ON' and check_gtid_con_res_slave=='ON' and check_gtid_mode_res_slave=='ON':
                        print("===============INFO:DATABASE RESTART COMPLETE====================")
                        pass
                    else:
                        print("The parameter of GTID not applied.Reset slave configure now.\n")
                        reset_slave(master_db_args, slave_db_args,master_cnf_dir,master_os_args,slave_os_args,slave_cnf_dir)
                        
                else:
                    print("Database restart failed.Reset slave configure now.")
                    reset_slave(master_db_args, slave_db_args,master_cnf_dir,master_os_args,slave_os_args,slave_cnf_dir)


            check_gtid_con_res = get_all(master_db_args, check_gtid_con_sql)[0][1]
            check_gtid_mode_res = get_all(master_db_args, check_gtid_mode_sql)[0][1]


            check_gtid_con_res_slave = get_all(
                slave_db_args, check_gtid_con_sql)[0][1]
            check_gtid_mode_res_slave = get_all(
                slave_db_args, check_gtid_mode_sql)[0][1]

            print(
                f"INFO:\nON MASTER\n----------------------\nEnforce_Gtid_Consistency:{check_gtid_con_res}")
            print(f"Gtid_Mode:{check_gtid_mode_res}\n")

            print(
                f"INFO:\nON SLAVE:\n----------------------\nEnforce_Gtid_Consistency:{check_gtid_con_res_slave}")
            print(f"Gtid_Mode:{check_gtid_mode_res_slave}\n")

            if check_gtid_con_res != 'ON' or check_gtid_mode_res != 'ON' or check_gtid_mode_res_slave != 'ON' or check_gtid_con_res_slave != 'ON':
                print("ERROR:\nThe configure of GTID failed.")
                reset_slave(master_db_args, slave_db_args,master_cnf_dir,master_os_args,slave_os_args,slave_cnf_dir)
                os._exit(0)
        else:
            print("ERROR:The version is 5.5 which unsupport for GTID!\n")
            reset_slave(master_db_args, slave_db_args,master_cnf_dir,master_os_args,slave_os_args,slave_cnf_dir)
            os._exit(0)

    # create replicate user on master
    create_user_res = create_rep_user(master_db_args, slave_db_args)
    if create_user_res == 'create user s':
        print("===============INFO:CREATE USER FOR REP COMPLETE====================")
        # start replicate on slave
        if 'semi' in mode:
            if 'rds' in mode:
                restart_s_res = restart_db_res = restart_db(slave_os_args,slave_db_args,slave_cnf_dir)
                if restart_s_res == 'restart s':
                    print("===============INFO:RESTART DATABASE COMPLETE====================")
                    start_slave_res = start_slave(master_db_args, slave_db_args, slave_os_args,mode)
                else:
                    reset_slave(master_db_args, slave_db_args,master_cnf_dir,master_os_args,slave_os_args,slave_cnf_dir)
                    os._exit(0)
            else:
                start_slave_res = start_slave(master_db_args, slave_db_args, slave_os_args,mode)
            time.sleep(5)
            if start_slave_res == 'start slave complete.':
                print(start_slave_res)
                print("===============INFO:START SLAVE PROCESS COMPLETE====================")
                master_add_param = ["plugin-load=rpl_semi_sync_master=semisync_master.so\n","rpl_semi_sync_master_enabled=1\n"]
                slave_add_param = ["plugin-load=rpl_semi_sync_slave=semisync_slave.so\n","rpl_semi_sync_slave_enabled=1\n"]
    
                master_cnf_text = ''.join(modify_cnf(m_text_list, master_add_param))
                slave_cnf_text = ''.join(modify_cnf(s_text_list, slave_add_param))
                if  'rds' not in mode:
                    ssh_input(
                        master_os_args, f'''mv {master_cnf_dir} {master_cnf_dir}.bak.add\necho """{master_cnf_text}""">>{master_cnf_dir}''')
                else:
                    pass
                ssh_input(
                    slave_os_args, f'''mv {slave_cnf_dir} {slave_cnf_dir}.bak.add\necho """{slave_cnf_text}""">>{slave_cnf_dir}''')
            else:
                print(start_slave_res)
                reset_slave(master_db_args, slave_db_args,master_cnf_dir,master_os_args,slave_os_args,slave_cnf_dir)
                os._exit(0)
                
        elif 'async' in mode:
            start_slave_res = start_slave(master_db_args, slave_db_args,slave_os_args, mode)
            time.sleep(5)

        # check db status
        time.sleep(5)
        set_readonly(slave_cnf_dir,slave_os_args,slave_db_args)
        rep_status(master_db_args, slave_db_args)

    else:
        pass

# mysql replicate : one master -- more slave
def mysql_rep_multi(master_db_args, master_os_args, master_cnf_dir, slave_db_args, slave_os_args, slave_cnf_dir, mode):
    

    # modify the mysql config file
    s_ori_text = ssh_input(slave_os_args, f"cat {slave_cnf_dir}")
    if 'normal' in mode:
        modify_text = []
        slave_text_list = modify_cnf(s_ori_text, modify_text)
        slave_cnf_text = ''.join(slave_text_list)

        ssh_input(slave_os_args,
              f'''mv {slave_cnf_dir} {slave_cnf_dir}_ori.bak\necho """{slave_cnf_text}""">>{slave_cnf_dir}''')
        pass
    elif 'gtid' in mode:

        m_version = get_all(master_db_args, "select version();")[0][0]
        s_version = get_all(slave_db_args, "select version();")[0][0]
        if m_version[0:3] == s_version[0:3] and m_version[0:3] != '5.5':

            if 'rds' in mode:
                modify_text = ['replicate-ignore-db=information_schema\n','replicate-ignore-db=performance_schema\n','replicate-ignore-db=mysql\n','replicate-ignore-db=sys\n','# GTID\n','gtid_mode=on\n', 'enforce_gtid_consistency=on\n', 'log-slave-updates=on\n']
            else:
                modify_text = ['# GTID\n','gtid_mode=on\n', 'enforce_gtid_consistency=on\n', 'log-slave-updates=on\n']
            slave_text_list = modify_cnf(s_ori_text, modify_text)
            slave_cnf_text = ''.join(slave_text_list)
            #modify_time = time.strftime("%Y-%m-%d-%H-%M", time.localtime())

            ssh_input(slave_os_args,
                f'''mv {slave_cnf_dir} {slave_cnf_dir}_ori.bak\necho """{slave_cnf_text}""">>{slave_cnf_dir}''')

            check_gtid_con_sql = "show global variables like 'enforce_gtid%';"
            check_gtid_mode_sql = "show global variables like 'gtid_mode%';"
            if m_version[0:3] == s_version[0:3] and m_version[0:3] != '5.6':
                set_con_warn = "SET @@GLOBAL.ENFORCE_GTID_CONSISTENCY = WARN;"
                set_con_on = "SET @@GLOBAL.ENFORCE_GTID_CONSISTENCY = ON;"
                set_mode_off = "SET @@GLOBAL.GTID_MODE = OFF_PERMISSIVE;"
                set_mode_on_p = "SET @@GLOBAL.GTID_MODE = ON_PERMISSIVE;"
                set_mode_on = "SET @@GLOBAL.GTID_MODE = ON;"

                get_all(slave_db_args, set_con_warn)
                get_all(slave_db_args, set_con_on)
                get_all(slave_db_args, set_mode_off)
                get_all(slave_db_args, set_mode_on_p)
                get_all(slave_db_args, set_mode_on)
            elif s_version[0:3] == '5.6':

                restart_s_res = restart_db_res = restart_db(slave_os_args,slave_db_args,slave_cnf_dir)
   
                if restart_s_res == 'restart s':
                    check_gtid_con_res = get_all(slave_db_args, check_gtid_con_sql)[0][1]
                    check_gtid_mode_res = get_all(slave_db_args, check_gtid_mode_sql)[0][1]
                    if check_gtid_con_res=='ON' and check_gtid_mode_res=='ON':
                        pass
                    else:
                        print("The parameter of GTID not applied.Reset slave configure now.\n")
                        reset_slave(master_db_args, slave_db_args,master_cnf_dir,master_os_args,slave_os_args,slave_cnf_dir)
                else:
                    print("Reset slave configure now.")
                    reset_slave(master_db_args, slave_db_args,master_cnf_dir,master_os_args,slave_os_args,slave_cnf_dir)


            check_gtid_con_res = get_all(slave_db_args, check_gtid_con_sql)[0][1]
            check_gtid_mode_res = get_all(slave_db_args, check_gtid_mode_sql)[0][1]


            check_gtid_con_res_slave = get_all(
                slave_db_args, check_gtid_con_sql)[0][1]
            check_gtid_mode_res_slave = get_all(
                slave_db_args, check_gtid_mode_sql)[0][1]

            print(
                f"INFO:\nON MASTER\n----------------------\nEnforce_Gtid_Consistency:{check_gtid_con_res}")
            print(f"Gtid_Mode:{check_gtid_mode_res}\n")

            print(
                f"INFO:\nON SLAVE:\n----------------------\nEnforce_Gtid_Consistency:{check_gtid_con_res_slave}")
            print(f"Gtid_Mode:{check_gtid_mode_res_slave}\n")

            if check_gtid_con_res != 'ON' or check_gtid_mode_res != 'ON' or check_gtid_mode_res_slave != 'ON' or check_gtid_con_res_slave != 'ON':
                print("ERROR:\nThe configure of GTID failed.")
                reset_slave(master_db_args, slave_db_args,master_cnf_dir,master_os_args,slave_os_args,slave_cnf_dir)
                os._exit(0)
        else:
            print("ERROR:The version is 5.5 which unsupport for GTID!\n")
            reset_slave(master_db_args, slave_db_args,master_cnf_dir,master_os_args,slave_os_args,slave_cnf_dir)
            os._exit(0)
    # create replicate user on master

    create_user_res = create_rep_user(master_db_args, slave_db_args)
    if create_user_res == 'create user s':
        # start replicate on slave
        if 'semi' in mode:
            if 'rds' in mode:
                restart_s_res = restart_db_res = restart_db(slave_os_args,slave_db_args,slave_cnf_dir)
                if restart_s_res == 'restart s':
                    start_slave_res = start_slave(master_db_args, slave_db_args, slave_os_args,mode)
                else:
                    reset_slave(master_db_args, slave_db_args,master_cnf_dir,master_os_args,slave_os_args,slave_cnf_dir)
                    os._exit(0)
            else:
                start_slave_res = start_slave(master_db_args, slave_db_args, slave_os_args,mode)
            time.sleep(5)
            if start_slave_res == 'start slave complete.':
                print(start_slave_res)

                slave_add_param = ["plugin-load=rpl_semi_sync_slave=semisync_slave.so\n","rpl_semi_sync_slave_enabled=1\n"]

                slave_cnf_text = ''.join(modify_cnf(slave_text_list, slave_add_param))
                ssh_input(
                    slave_os_args, f'''mv {slave_cnf_dir} {slave_cnf_dir}.bak.add\necho """{slave_cnf_text}""">>{slave_cnf_dir}''')
            else:
                print(start_slave_res)
                reset_slave(master_db_args, slave_db_args,master_cnf_dir,master_os_args,slave_os_args,slave_cnf_dir)
                os._exit(0)
                
        elif 'async' in mode:
            start_slave_res = start_slave(master_db_args, slave_db_args, slave_os_args,mode)

        # check db status
        time.sleep(5)
        set_readonly(slave_cnf_dir,slave_os_args,slave_db_args)
        rep_status(master_db_args, slave_db_args)

    else:
        pass

    return slave_db_args[0]