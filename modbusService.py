from pymodbus.client.sync import ModbusTcpClient
import pymssql
import sys
import _mssql
import time
import os
from pytz import timezone
from datetime import datetime
from pymodbus.register_read_message import ReadHoldingRegistersResponse

hostname = "192.168.0.001"  # ip
port = 1774
check = 0

def ping_reboot():
    fmt = '%Y-%m-%d %H:%M:%S '
    KST = datetime.now(timezone('Asia/Seoul'))
    response = os.system("ping -c 1 " + hostname)
    if response == 0:
        return check
    else:
        with open('./net.txt', 'a') as wf:
            wf.write(KST.strftime(fmt)+' ----------- down!\n')
        time.sleep(3)
        if check > 10:
            os.system("sudo reboot now")
        return check+1

with open("connect_env.txt", 'r') as fr :
    buf_lines = fr.readlines()
    row_server = buf_lines[0:1]
    row_user = buf_lines[1:2]
    row_password = buf_lines[2:3]
    row_database = buf_lines[3:4]
    row_machine = buf_lines[4:5]
    
    server = row_server[0].replace('server = ', '')
    user = row_user[0].replace('user = ', '')
    password = row_password[0].replace('password = ', '')
    database = row_database[0].replace('database = ', '')
    mchcd = (row_machine[0].replace('machine = ', ''))[:-2]
proc_mchcd = str("'%s'" % mchcd) 

try:
    conn = pymssql.connect(server[:-2], user[:-2], password[:-2], database[:-2], timeout = 3)
    cursor = conn.cursor()
    cursor.execute('proc_name' % proc_mchcd)
    time.sleep(0.1)
    row = cursor.fetchone()
    conn.commit()
    conn.close()
    time.sleep(1)
    row_list = list(row)
    address_list = row_list[3:]
    a = address_list.index(None)
    b = address_list[0:a]
    reg_no = []
    
    while True :
        client = None
        client = ModbusTcpClient(host, port)
        client.close()
        client.connect()
   
        if client.connect():
	        check = ping_reboot()
   
        reg_data = []
            
        for i in range(0, a):
            time.sleep(0.3)
            d = int(b[i])
            rr = client.read_holding_registers(d,1, unit = 1)
            assert(rr.function_code < 0x80)

        if isinstance(rr, ReadHoldingRegistersResponse):
	        temp = rr.registers
        else:
            with open('./log_data.txt','a') as log:
                log.write('isinstance error '+str(datetime.now()) +'\n')
                reg_data.append(rr.registers[0])
            reg_data.insert(0, mchcd)
            prod_data = tuple(reg_data)
            time.sleep(0.3)
            conn = pymssql.connect(server[:-2], user[:-2], password[:-2], database[:-2], timeout = 3)
            cursor = conn.cursor()
            cursor.callproc('proc_name', prod_data)
            time.sleep(0.3)
            conn.commit()
            conn.close()
        client.close()
        client = None
        time.sleep(5)
    else:
	    client.close()
	    client = None
	    os.execl(sys.executable,sys.executable,*sys.argv)
except _mssql.MssqlDatabaseException as ae:
    os.system("cd /home/pi/Python-ModbusService")
    os.execl(sys.executable,sys.executable,*sys.argv)
except pymssql.OperationalError as oe:
    os.system("cd /home/pi/Python-ModbusService")
    os.execl(sys.executable,sys.executable,*sys.argv)
except Exception as e:
    if not conn is None:
        conn.close()
    client.close()
    client = None
    now = datetime.now()
    with open('./log_data.txt','a') as log:
        log.write(str(e) + str(now) + '\n')
    time.sleep(30)
    os.system("cd /home/pi/Python-ModbusService")
    os.execl(sys.executable,sys.executable,*sys.argv)
