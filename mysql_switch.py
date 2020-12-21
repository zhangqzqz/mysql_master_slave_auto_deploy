import os
from method import create_rep_user, modify_cnf, start_slave, rep_status,reset_slave,get_pos,set_readonly,check_switch,check_pos,set_readonly_master
from ssh_input import ssh_input
import time
from mysql_conn import get_all, get_dict,run_noprint,more_sql
from config_parse import reset_config,get_config,get_slaves

# mysql switch master-slave
def mysql_switch(master_db_args, master_os_args, master_cnf_dir, slave_db_args, slave_os_args, slave_cnf_dir,slaves,cnf):
    check_switch_res = check_switch(master_db_args, slaves)
    if 'WARNING' in check_switch_res:
        print("WARNING:It is not recommended to perform master-slave switching.\n")
        print(check_switch_res)
        os._exit(0)
    else:
        print("INFO:The check is passed, the master-slave switch can be performed.\n")
        # set master read only on
        set_readonly_master(master_db_args,'on',master_cnf_dir,master_os_args)
        # check status 5 times,2 minutes apart
        freq = 0
        while freq < 20:
            check_pos_res = check_pos(master_db_args,slaves)
            if 'Unsynchronized' in check_pos_res:
                print("WARNING:There have slave db were Unsynchronized,check again after 10s.")
                time.sleep(10)
                freq += 1
            else:
                break
        # if check pos failed set master read only off,else stop slave on every slave db
        if 'Unsynchronized' in check_pos_res:
            print("WANING:The status of position were Unsynchronized.\n")
            set_readonly_master(master_db_args,'off',master_cnf_dir,master_os_args)
            os._exit(0)
        else:
            # choose the slave u want to switch as master
            try:
                switch_num = int(input("Please choose a slave db which you want to switch as the new master (input the serial number such as :1,2,3...):\n"))
            except:
                set_readonly_master(master_db_args,'off',master_cnf_dir,master_os_args)
            if switch_num == 0 or switch_num > len(slaves):
                print("WARNING:Please input a vaild serial number!")
                set_readonly_master(master_db_args,'off',master_cnf_dir,master_os_args)
                os._exit(0)
            switch_num -= switch_num
            for slave in slaves:
                slave_db_args = slave[0]
                run_noprint(slave_db_args,'stop slave;')
            # set switch on slave A
            slave_db_args_a = slaves[switch_num][0]
            slave_os_args_a = slaves[switch_num][1]
            slave_a_ip = slave_db_args_a[0]
            slave_a_port = str(slave_db_args_a[2])
            slave_cnf_dir_a = slaves[switch_num][2]
            # username = 'mcbak_' + master_db_args[0].split('.')[-1] + '_' +str(master_db_args[2])
            username = 'np_rep'
            run_noprint(slave_db_args_a,'reset slave all;')
            set_readonly_master(slave_db_args_a,'off',slave_cnf_dir_a,slave_os_args_a)
            master_status_res = get_dict(slave_db_args_a,'show master status;')[0]

            switch_file = master_status_res['File']
            switch_pos = master_status_res['Position']
            # create sync user on slave A:
            print("INFO:configure switch for old master db:\n ")
            create_rep_user(slave_db_args_a,master_db_args)
            # change master pos on old master
            run_noprint(master_db_args,'stop slave;')
            run_noprint(master_db_args,'reset slave all;')
            run_noprint(master_db_args,'change master to master_auto_position=0;')
            change_old_master_sql = f"change master to master_host='{slave_a_ip}',master_user='{username}',master_password=npbackup',\
                master_port={slave_a_port},master_log_file='{switch_file}',master_log_pos={switch_pos};"
            print(change_old_master_sql)
            more_sql(master_db_args,[change_old_master_sql,'start slave;'])
            

            # set switch on other slaves
            if len(slaves) != 1:
                slaves.remove(slaves[switch_num])
                for slave in slaves:
                    slave_db_args = slave[0]
                    slave_os_args = slave[1]
                    slave_cnf_dir = slave[2]
                    # username = 'mcbak_' + slave_db_args[0].split('.')[-1] + '_' +str(slave_db_args[2])
                    username = 'np_rep'
                    run_noprint(slave_db_args,'stop slave;')
                    run_noprint(slave_db_args,'change master to master_auto_position=0;')
                    change_master_sql = f"change master to master_host='{slave_a_ip}',master_user='{username}',master_password='npbackup',\
                master_port={slave_a_port},master_log_file='{switch_file}',master_log_pos={switch_pos};"
                    create_rep_user(slave_db_args_a,slave_db_args)
                   # run_noprint(slave_db_args,'reset slave all;')
                    set_readonly_master(slave_db_args,'on',slave_cnf_dir,slave_os_args)
                    more_sql(slave_db_args,[change_master_sql,'start slave;'])
                
            # reset config in config.cfg base on new master-slave
            reset_config(switch_num,cnf)
            print("INFO:Reconfig the configure in config.cfg\n")
            print("INFO:Switch complete.Check status now.\n")

            master_db_args, master_os_args, master_cnf_dir ,xbakup_dir = get_config('master_config',cnf)
            slaves = get_slaves(cnf)
            check_switch_res = check_switch(master_db_args, slaves)


            return "switch complete."
        
        return 'switch failed.'
