import paramiko
import time
import os
import getpass

ssh = paramiko.SSHClient()
"""
    **  This is a custom HiveUI that runs Hive queries directly using command line arguements
    **  Only output on console is printed, job status is not shown but however it can viewed
    **  in ResourceManager portal of the Hadoop cluster.
    
    **  Inputs:
    **  Ip-address, Username, Password, Database
"""

def run_command_on_device(ip_address, username, password, database):
    """ Connect to a device, run a command, and return the output."""

    # Load SSH host keys.
    ssh.load_system_host_keys()
    # Add SSH host key when missing.
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    total_attempts = 3
    for attempt in range(total_attempts):
        try:
            print("Attempt to connect: %s" % attempt)
            # Connect to node using username/password authentication.
            ssh.connect(ip_address, 
                        username=username, 
                        password=password,
                        look_for_keys=False )
            time.sleep(5)
            print("Authenticated!")

            while True:
                # Take input of Hive Query
                command=input("hive> ")
                if command=="exit;":
                    break
            
                #Run the query using command line arguements
                command_to_run="hive --database "+database+" -e "+"'"+command+"'"
                ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(command_to_run)
                output = ssh_stdout.readlines()
                
                #Display Output
                for line in output:
                    print(line)
                    
            # Close connection.
            ssh.close()
            break

        except Exception as error_message:
            print("Unable to connect")
            print(error_message)


# Run function
router_ip=input("IP Address: ")
router_username=input("Username: ")
router_password=getpass.getpass("Password: ")
database_to_use=input("Database: ")
run_command_on_device(router_ip, router_username, router_password, database_to_use)
