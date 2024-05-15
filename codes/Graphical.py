import paramiko
import sys
import matplotlib.pyplot as plt
import numpy as np
import time
import getpass

ssh = paramiko.SSHClient()
font1 = {'family':'serif','color':'blue','size':20}
font2 = {'family':'serif','color':'darkred','size':15}

"""
    **  This application is a tool to generate graphs to perform graphical analysis on OLAP data.
    **  Input:
    **  Ip Address, Username, Password, Database, City, First year to compare, Second year to compare, Parameter to compare
    
    **  Hive Command line is used to fetch data stored in HDFS.
    
    **  Python matplotlib package is used to present data on the graphical form
"""

def display_graph(ip_address, username, password, database, first_year, second_year, month, parameter, city):
     # Load SSH host keys.
    ssh.load_system_host_keys()
    # Add SSH host key when missing.
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    total_attempts = 3

    for attempt in range(1,total_attempts+1):
        try:
            print("Attempt to connect: %s" % attempt)
            # Connect to router using username/password authentication.
            ssh.connect(ip_address, 
                        username=username, 
                        password=password,
                        look_for_keys=False )
            # Run command.
            time.sleep(5)
            print("Authenticated!")
            months=["January","February","March","April","May","June","July","August","September","October","November","December"]
            
            # Specific for each month since number of days vary for each month
            match month:
                case 1 | 3 | 5 | 7 | 8 | 10 | 12:
                    #print(1)
                    parameter1=parameter.replace(" ","_")
                    #Prepare command
                    command="select "+parameter1+" from "+city

                    #Run the command
                    command_to_run="hive --database "+database+" -e "+"'"+command+"'"
                    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(command_to_run)
                    output = ssh_stdout.readlines()
                    
                    #Split output for Matplotlib to plot
                    i=0
                    l=[]
                    m=[]
                    days_of_week=[i for i in range(1,32)]
                    for line in output:
                        if(i<31):
                            l.append(float(line))
                            i+=1
                        else:
                            m.append(float(line))    
                    
                    #Create np arrays
                    x = np.array(days_of_week)
                    y1 = np.array(l)
                    y2 = np.array(m)
                    
                    #Plot the data
                    plt.plot(x, y1, 'o:r',mec='g',mfc='b',label=first_year)
                    plt.plot(x, y2, 'o--c',mec='m',mfc='y',label=second_year)
                    plt.xlabel("Days of the month",fontdict=font2)
                    plt.ylabel("Value",fontdict=font2)
                    plt.title(parameter+" for "+months[month-1], fontdict=font1)
                    plt.grid(axis = 'x')
                    plt.legend(loc="upper right")
                    plt.show()
                    break
                case 4 | 6 | 9 | 11:
                    parameter1=parameter.replace(" ","_")
                    command="select "+parameter1+" from "+city
                    command_to_run="hive --database "+database+" -e "+"'"+command+"'"
                    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(command_to_run)
                    output = ssh_stdout.readlines()
                    i=0
                    l=[]
                    m=[]
                    days_of_week=[i for i in range(1,31)]
                    for line in output:
                        if(i<30):
                            l.append(float(line))
                            i+=1
                        else:
                            m.append(float(line))    

                    x = np.array(days_of_week)
                    y1 = np.array(l)
                    y2 = np.array(m)
                    plt.plot(x, y1, 'o:r',mec='g',mfc='b',label=first_year)
                    plt.plot(x, y2, 'o--c',mec='m',mfc='y',label=second_year)
                    plt.xlabel("Days of the month",fontdict=font2)
                    plt.ylabel("Value",fontdict=font2)
                    plt.title(parameter+" for "+months[month-1], fontdict=font1)
                    plt.grid(axis = 'x')
                    plt.legend(loc="upper right")
                    plt.show()
                    break
                case 2:
                    parameter1=parameter.replace(" ","_")
                    command="select "+parameter1+" from "+city
                    command_to_run="hive --database "+database+" -e "+"'"+command+"'"
                    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(command_to_run)
                    output = ssh_stdout.readlines()
                    i=0
                    l=[]
                    m=[]
                    days_of_week=[i for i in range(1,29)]
                    for line in output:
                        if(i<28):
                            l.append(float(line))
                            i+=1
                        else:
                            m.append(float(line))    

                    x = np.array(days_of_week)
                    y1 = np.array(l)
                    y2 = np.array(m)
                    plt.plot(x, y1, 'o:r',mec='g',mfc='b', label=first_year)
                    plt.plot(x, y2, 'o--c',mec='m',mfc='y', label=second_year)
                    plt.xlabel("Days of the month",fontdict=font2)
                    plt.ylabel("Value",fontdict=font2)
                    plt.title(parameter+" for "+months[month-1], fontdict=font1)
                    plt.grid(axis = 'x')
                    plt.legend(loc="upper right")
                    plt.show()
                    break
                case default:
                    raise Exception("Invalid Month entry")
                    
                # Close connection.
            ssh.close()

        except Exception as error_message:
            print("Unable to connect")
            print(error_message)

# Run function
router_ip=input("IP Address: ")
router_username=input("Username: ")
router_password=getpass.getpass("Password: ")
database_to_use=input("Database: ")
start_year=int(input("Enter the first year"))
end_year=int(input("Enter the second year"))
print("Enter the month: ")
print("01. Jan")
print("02. Feb")
print("03. March")
print("04. April")
print("05. May")
print("06. June")
print("07. July")
print("08. August")
print("09. September")
print("10. October")
print("11. November")
print("12. December")
month=int(input())
parameter=input("Enter parameter (like temperature): ")
city=input("Enter city (like nyc): ")
display_graph(router_ip, router_username, router_password, database_to_use, start_year, end_year, month, parameter, city)
