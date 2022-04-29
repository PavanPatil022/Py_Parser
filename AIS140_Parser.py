import tkinter as tk
from tkinter import filedialog
from tqdm import tqdm
import datetime
import csv
import pandas as pandasForSortingCSV
import pandas as pd
import time

start_time = datetime.datetime.now()
date = datetime.datetime.now().strftime("%Y%m%d%I%M%S%p")

typeofproto = ['Binary', 'AIS140']
print("Type of Protocols:")
print("1 ",typeofproto[0])
print("2 ",typeofproto[1])
proto_type = typeofproto[int(input("Enter the protocol index no to select the type of protocol for parsing: ")) - 1]
print("Selected protocol type is: ", proto_type)

if proto_type == 'AIS140':
    print("Select the AIS140 log file for data analysis......")
    time.sleep(2)
    root = tk.Tk()
    root.withdraw()
    filename = filedialog.askopenfilename()

    with open(filename) as file:
        lines = file.readlines()
        lines = [line.rstrip() for line in lines]

    # imei = input("Enter the Device IMEI Number: ")
    available_imei = []
    parsed_file = date + "_Parsed_data.csv"

    with open(parsed_file, 'w+', newline='') as csvfile:
        header = ['Header', 'Vendor_ID', 'FW_Version', 'Packet_Type', 'Alert_ID', 'Live/History', 'IMEI', 'Vehicle_No',
                  'GPS_fix', 'Date', 'Time', 'Lat', 'Lat_Dir', 'Long', 'Long_Dir', 'Temp1', 'Temp2', 'Temp3', 'Temp4',
                  'Temp5', 'Temp6', 'Profile', 'IGN_Status', 'Temp9', 'EXT_BAT', 'INT_BAT', 'EMR_Status', 'Case_Status',
                  'Temp10', 'Temp11', 'Temp12', 'Temp13', 'Temp14', 'Temp15', 'Temp16', 'Temp17', 'Temp18', 'Temp19',
                  'Temp20', 'Temp21', 'Temp22', 'Temp23', 'Temp24', 'Temp25', 'Temp26', 'DIN', 'DOUT', 'sequence_no',
                  'AIN1', 'AIN2', 'AIN3', 'Delta_Dist', 'OTA', 'Checksum']
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(header)
        for i in tqdm(range(len(lines)), desc='Extracting available IMEI numbers......'):
            if len(lines[i]) > 200:
                temp = lines[i].split(',')
                if temp[6] not in available_imei:
                    available_imei.append(temp[6])

        print("Available Imei Numbers are: ")
        for i in range(len(available_imei)):
            print(i + 1, available_imei[i])
        imei = available_imei[int(input("Enter the IMEI index no of available IMEI: ")) - 1]
        print("Selected IMEI Number is: ", imei)
        for i in tqdm(range(len(lines)), desc='Parsing the data.......'):
            # print(len(lines[i]))
            if len(lines[i]) > 200:
                data = lines[i].split(',')
                if len(data) > 53:
                    string = data[52] + ':' + data[53] + ':' + data[54]
                    data[52] = string
                    del data[53:55]
                data2 = data[52].split('*')
                data[52] = data2[0]
                data.append(data2[1])
                if data[6] == imei:
                    csvwriter.writerow(data)
        csvfile.close()

    csvData = pandasForSortingCSV.read_csv(parsed_file)

    # print("\nBefore sorting:")
    # print(csvData)

    csvData.sort_values(["sequence_no"],
                        axis=0,
                        ascending=[True],
                        inplace=True)

    # print("\nAfter sorting:")
    # print(csvData)
    # print(type(csvData))
    csvData_reset = csvData.reset_index()
    csvData_reset.to_csv(parsed_file)
    seq = csvData_reset.sequence_no

    timestamp = pd.read_csv(parsed_file, usecols=['Alert_ID', 'Date', 'Time', 'IGN_Status', 'EMR_Status', 'Delta_Dist'])
    profile = csvData_reset.Profile
    timestamp.Time = timestamp.Time.astype(str)
    emr_status = timestamp.EMR_Status.astype(str)
    ign_status = timestamp.IGN_Status.astype(str)
    alert_id = timestamp.Alert_ID.astype(str)
    distance = timestamp.Delta_Dist
    #print(distance[0])
    Total_dist1 = []
    typeofdist = ['odometer', 'delta distance']
    print("Type of distance calculation:")
    print("1 ",typeofdist[0])
    print("2 ",typeofdist[1])
    dist_type = typeofdist[int(input("Enter the distance index no to select the type of distance calculation: ")) - 1]
    print("Selected distance calculation type is: ", dist_type)
    last_index = len(distance) - 1
    Total_dist = 0
    if dist_type == 'odometer':
        Total_dist = distance[last_index]-distance[0]
        #print("dist: ",Total_dist)
    elif dist_type == 'delta distance':
        for i in range(len(distance)):
            if i < last_index:
                diff = seq[i+1]-seq[i]
            if diff != 0:
                Total_dist = Total_dist + distance[i]
    else:
        print("Select a valid distance type")
        #print("dist", Total_dist)

    ign_on = int(input("Enter the Ignition ON interval: "))
    ign_off = int(input("Enter the Ignition OFF interval: "))
    emr_on = int(input("Enter the Emergency ON interval: "))
    alert_flag1 = 0
    alert_flag2 = 0
    # print(timestamp.Time[1])
    # a = '0' + timestamp.Time[1]
    # print(len(timestamp.Time[1]))
    # print(a)
    x = 0
    data1 = []
    dublicate_packet = 0
    dublicate_packet1 = []
    data_loss = 0
    data_loss1 = []

    xflag = 0
    result_file = date + "_Result.csv"
    with open(result_file, 'w+', newline='') as csvfile1:
        header = ['IMEI', 'Curent_Alert_ID', 'Curent_Seq_no', 'Date', 'Time', 'Previous_Alert_ID', 'Previous_Seq_no', 'Date', 'Time', 'Data_loss', 'Result']
        csvwriter1 = csv.writer(csvfile1)
        csvwriter1.writerow(header)
        for x in tqdm(range(len(seq) - 1), desc='Performing Operations.......'):

            y = x + 1

            dif = seq[y] - seq[x]
            #print(dif)
            prof = profile[x]
            #print(prof)
            #check profile mismatch
            if (prof != 'airtel'):
                if (prof != 'IND airtel'):
                    #print("in airtel")
                    if 'BSNL Mobile' != prof:
                        #print("in bsnl")
                        data1.append(imei)
                        data1.append(alert_id[x])
                        data1.append(seq[x])
                        data1.append(timestamp.Date[x])
                        data1.append(timestamp.Time[x])
                        data1.append('NA')
                        data1.append('NA')
                        data1.append('NA')
                        data1.append('NA')
                        data1.append(profile[x])
                        data1.append('Profile mismatch')
                        csvwriter1.writerow(data1)
                        data1 = []
            #print("sequence")
            #check data loss
            if dif > 1:
                #print("Data loss")
                data1.append(imei)
                data1.append(alert_id[y])
                data1.append(seq[y])
                data1.append(timestamp.Date[y])
                data1.append(timestamp.Time[y])
                data1.append(alert_id[x])
                data1.append(seq[x])
                data1.append(timestamp.Date[x])
                data1.append(timestamp.Time[x])
                data1.append(dif - 1)
                data1.append('Packet loss')
                #print("dif= ",dif-1)
                data_loss = data_loss + dif - 1
                csvwriter1.writerow(data1)
                data1 = []
            #check dublicate packet
            elif dif == 0:
                #print("dublicate")
                data1.append(imei)
                data1.append(alert_id[y])
                data1.append(seq[y])
                data1.append(timestamp.Date[y])
                data1.append(timestamp.Time[y])
                data1.append(alert_id[x])
                data1.append(seq[x])
                data1.append(timestamp.Date[x])
                data1.append(timestamp.Time[x])
                data1.append(dif)
                data1.append('Dublicate Packet')
                dublicate_packet = dublicate_packet + 1
                csvwriter1.writerow(data1)
                data1 = []

            #packet frequency verification
            if xflag == 0:
                y = x + 1
                dif = seq[y] - seq[x]
            elif xflag == 1:
                x = x - 1
                y = x + 2
            #seperating alert and normal packets
            if alert_id[x] == '1':
                alert_flag1 = 1
            elif alert_id[x] == '2':
                alert_flag1 = 1
            elif alert_id[x] == '16':
                alert_flag1 = 1
            else:
                alert_flag1 = 0

            if alert_id[y] == '1':
                alert_flag2 = 1
            elif alert_id[y] == '2':
                alert_flag2 = 1
            elif alert_id[y] == '16':
                alert_flag2 = 1
            else:
                alert_flag2 = 0

            #print("time")
            #Both packets are normal packet
            if alert_flag1 == 1 and alert_flag2 == 1:
                #print(" time 11")
                #print("alert1: ", alert_id[x])
                #print("alert2: ", alert_id[y])
                temp1 = timestamp.Time[x]
                if len(temp1) != 6:
                    while (1):
                        if len(temp1) == 6:
                            break
                        else:
                            temp1 = '0' + temp1
                # print("After: ",temp1)
                hh1 = int(temp1[0:2])
                mm1 = int(temp1[2:4])
                ss1 = int(temp1[4:6])

                temp2 = timestamp.Time[y]
                if len(temp2) != 6:
                    while (1):
                        if len(temp2) == 6:
                            break
                        else:
                            temp2 = '0' + temp2
                hh2 = int(temp2[0:2])
                mm2 = int(temp2[2:4])
                ss2 = int(temp2[4:6])

                # print("time1: ", temp1)
                temp1_sec = (hh1 * 3600) + (mm1 * 60) + ss1
                temp2_sec = (hh2 * 3600) + (mm2 * 60) + ss2
                time_dif = temp2_sec - temp1_sec
                #print(emr_status[x])
                #print(type(emr_status[x]))
                if emr_status[x] != '1':
                    #checking emergency status
                    if ign_status[x] == '1':
                        #checking ignition status
                        if time_dif != ign_on:
                            #checking for time diffrence
                            data1.append(imei)
                            data1.append(alert_id[y])
                            data1.append(seq[y])
                            data1.append(timestamp.Date[y])
                            data1.append(timestamp.Time[y])
                            data1.append(alert_id[x])
                            data1.append(seq[x])
                            data1.append(timestamp.Date[x])
                            data1.append(timestamp.Time[x])
                            data1.append(time_dif)
                            data1.append('Ign ON time difference')
                            csvwriter1.writerow(data1)
                            data1 = []
                            alert_flag1 = 0
                            alert_flag2 = 0
                    elif ign_status[x] == '0':
                        if time_dif != ign_off:
                            data1.append(imei)
                            data1.append(alert_id[y])
                            data1.append(seq[y])
                            data1.append(timestamp.Date[y])
                            data1.append(timestamp.Time[y])
                            data1.append(alert_id[x])
                            data1.append(seq[x])
                            data1.append(timestamp.Date[x])
                            data1.append(timestamp.Time[x])
                            data1.append(time_dif)
                            data1.append('Ign OFF time difference')
                            csvwriter1.writerow(data1)
                            data1 = []
                            alert_flag1 = 0
                            alert_flag2 = 0
                elif emr_status[x] == '1':
                    # print("in emr")
                    # print("time dif: ",time_dif,"emr on: ",emr_on)
                    if time_dif != emr_on:
                        data1.append(imei)
                        data1.append(alert_id[y])
                        data1.append(seq[y])
                        data1.append(timestamp.Date[y])
                        data1.append(timestamp.Time[y])
                        data1.append(alert_id[x])
                        data1.append(seq[x])
                        data1.append(timestamp.Date[x])
                        data1.append(timestamp.Time[x])
                        data1.append(time_dif)
                        data1.append('EMR ON time difference')
                        csvwriter1.writerow(data1)
                        data1 = []
                        alert_flag1 = 0
                        alert_flag2 = 0
            elif (alert_flag1 == 1 and alert_flag2 == 0) or (alert_flag1 == 0 and alert_flag2 == 1):
                #print("Time 10 01")
                temp1 = timestamp.Time[x]
                # print("before: ",temp1)
                if len(temp1) != 6:
                    while (1):
                        if len(temp1) == 6:
                            break
                        else:
                            temp1 = '0' + temp1
                # print("After: ",temp1)
                hh1 = int(temp1[0:2])
                mm1 = int(temp1[2:4])
                ss1 = int(temp1[4:6])

                temp2 = timestamp.Time[y]
                if len(temp2) != 6:
                    while (1):
                            if len(temp2) == 6:
                                break
                            else:
                                temp2 = '0' + temp2
                # print("After: ",temp1)
                hh2 = int(temp2[0:2])
                mm2 = int(temp2[2:4])
                ss2 = int(temp2[4:6])

                temp1_sec = (hh1 * 3600) + (mm1 * 60) + ss1
                temp2_sec = (hh2 * 3600) + (mm2 * 60) + ss2
                time_dif = temp2_sec - temp1_sec
                data1.append(imei)
                data1.append(alert_id[y])
                data1.append(seq[y])
                data1.append(timestamp.Date[y])
                data1.append(timestamp.Time[y])
                data1.append(alert_id[x])
                data1.append(seq[x])
                data1.append(timestamp.Date[x])
                data1.append(timestamp.Time[x])
                data1.append(time_dif)
                data1.append('Alert Event')
                csvwriter1.writerow(data1)
                data1 = []
                if alert_flag1 == 1:
                    xflag = 1
                alert_flag1 = 0
                alert_flag2 = 0
            elif (alert_flag1 == 0 and alert_flag2 == 0):
                #print("time 00")
                temp1 = timestamp.Time[x]
                # print("before: ",temp1)
                if len(temp1) != 6:
                    while (1):
                        if len(temp1) == 6:
                            break
                        else:
                            temp1 = '0' + temp1
                # print("After: ",temp1)
                hh1 = int(temp1[0:2])
                mm1 = int(temp1[2:4])
                ss1 = int(temp1[4:6])

                temp2 = timestamp.Time[y]
                if len(temp2) != 6:
                    while (1):
                            if len(temp2) == 6:
                                break
                            else:
                                temp2 = '0' + temp2
                # print("After: ",temp1)
                hh2 = int(temp2[0:2])
                mm2 = int(temp2[2:4])
                ss2 = int(temp2[4:6])

                temp1_sec = (hh1 * 3600) + (mm1 * 60) + ss1
                temp2_sec = (hh2 * 3600) + (mm2 * 60) + ss2
                time_dif = temp2_sec - temp1_sec
                data1.append(imei)
                data1.append(alert_id[y])
                data1.append(seq[y])
                data1.append(timestamp.Date[y])
                data1.append(timestamp.Time[y])
                data1.append(alert_id[x])
                data1.append(seq[x])
                data1.append(timestamp.Date[x])
                data1.append(timestamp.Time[x])
                data1.append(time_dif)
                data1.append('Alert Event')
                csvwriter1.writerow(data1)
                data1 = []
                alert_flag1 = 0
                alert_flag2 = 0



        data_loss1.append('Total_data_loss= ')
        data_loss1.append(data_loss)
        dublicate_packet1.append('Total_dublicate= ')
        dublicate_packet1.append(dublicate_packet)
        Total_dist1.append('Total_dist= ')
        Total_dist1.append(Total_dist)
        csvwriter1.writerow(data_loss1)
        csvwriter1.writerow(dublicate_packet1)
        csvwriter1.writerow(Total_dist1)

    print("-------------Completed----------")
    time.sleep(10)
elif proto_type == 'Binary':

    typeofserver = ['Dev', 'ALUAT']
    print("Type of Server:")
    print("1 ", typeofserver[0])
    print("2 ", typeofserver[1])
    server_type = typeofserver[int(input("Enter the Server index no to select the type of server for parsing: ")) - 1]
    print("Selected server type is: ", server_type)

    if server_type == 'Dev':
        print("Select the Binary log file for data analysis......")
        #time.sleep(2)
        root = tk.Tk()
        root.withdraw()
        filename = filedialog.askopenfilename()

        with open(filename) as file:
            lines = file.readlines()
            lines = [line.rstrip() for line in lines]

        # imei = input("Enter the Device IMEI Number: ")
        #print(lines)
        lines1 = []
        for i in range(len(lines)-1):
            temp = lines[i].split(':')
            lines1.append(temp)
        #print(lines1)
        lines2 = []

        available_imei = []
        parsed_file = date + "_Parsed_data.csv"

        with open(parsed_file, 'w+', newline='') as csvfile:
            header = ['Header', 'DBOM1', 'IMEI', 'DBOM2', 'seq_no', '','','','','','','','','','','','','','','','','','',
                      '','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','',
                      '','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','',
                      '','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','',
                      '','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','',
                      '','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','',
                      '','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','',
                      '','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','',
                      '','','','','','',]
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(header)
            for i in tqdm(range(len(lines1)), desc='Extracting data......'):
                temp = lines1[i]
                #print('temp = ',temp)
                if len(temp) > 50:
                    temp3 = []
                    flat_list = []
                    for j in range(len(temp)):
                        try:
                            temp2 = temp[j].split(',')
                            temp3.append(temp2)
                        except:
                            temp3.append(temp[j])
                    flat_list = [item for sublist in temp3 for item in sublist]
                    #print('ft= ',flat_list)
                    imei = flat_list[2]
                    csvwriter.writerow(flat_list)
            csvfile.close()

        csvData = pandasForSortingCSV.read_csv(parsed_file)

        # print("\nBefore sorting:")
        # print(csvData)

        csvData.sort_values(["seq_no"],
                            axis=0,
                            ascending=[True],
                            inplace=True)

        # print("\nAfter sorting:")
        # print(csvData)
        # print(type(csvData))
        csvData_reset = csvData.reset_index()
        csvData_reset.to_csv(parsed_file)
        seq = csvData_reset.seq_no

        data1 = []
        dublicate_packet = 0
        dublicate_packet1 = []
        data_loss = 0
        data_loss1 = []
        result_file = date + "_Result.csv"
        with open(result_file, 'w+', newline='') as csvfile1:
            header = ['IMEI', 'Curent_Seq_no', 'Previous_Seq_no', 'Data_loss', 'Result']
            csvwriter1 = csv.writer(csvfile1)
            csvwriter1.writerow(header)
            for x in tqdm(range(len(seq) - 1), desc='Performing Operations.......'):
                y = x + 1

                dif = seq[y] - seq[x]
                if dif > 1:
                    #print("Data loss")
                    data1.append(imei)
                    data1.append(seq[y])
                    data1.append(seq[x])
                    data1.append(dif - 1)
                    data1.append('Packet loss')
                    data_loss = data_loss + dif - 1
                    csvwriter1.writerow(data1)
                    data1 = []
                #check dublicate packet
                elif dif == 0:
                    data1.append(imei)
                    data1.append(seq[y])
                    data1.append(seq[x])
                    data1.append(dif)
                    data1.append('Dublicate Packet')
                    dublicate_packet = dublicate_packet + 1
                    csvwriter1.writerow(data1)
                    do = 'nothing'
                    data1 = []

            data_loapss1.pend('Total_data_loss= ')
            data_loss1.append(data_loss)
            dublicate_packet1.append('Total_dublicate= ')
            dublicate_packet1.append(dublicate_packet)
            csvwriter1.writerow(data_loss1)
            csvwriter1.writerow(dublicate_packet1)
    elif server_type == 'ALUAT':
        print("Select the Binary log file for data analysis......")
        # time.sleep(2)
        root = tk.Tk()
        root.withdraw()
        filename = filedialog.askopenfilename()

        with open(filename) as file:
            lines = file.readlines()
            lines = [line.rstrip() for line in lines]

        # imei = input("Enter the Device IMEI Number: ")
        # print(lines)
        lines1 = []
        for i in range(len(lines) - 1):
            temp = lines[i].split(':')
            lines1.append(temp)
        # print(lines1)
        lines2 = []

        available_imei = []
        parsed_file = date + "_Parsed_data.csv"

        with open(parsed_file, 'w+', newline='') as csvfile:
            header = ['Header', 'IMEI', 'DBOM2', 'seq_no', '', '', '', '', '', '', '', '', '', '', '', '', '',
                      '', '', '', '', '',
                      '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '',
                      '', '', '', '', '', '', '', '', '', '',
                      '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '',
                      '', '', '', '', '', '', '', '', '', '',
                      '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '',
                      '', '', '', '', '', '', '', '', '', '',
                      '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '',
                      '', '', '', '', '', '', '', '', '', '',
                      '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '',
                      '', '', '', '', '', '', '', '', '', '',
                      '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '',
                      '', '', '', '', '', '', '', '', '', '',
                      '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '',
                      '', '', '' ]
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(header)
            for i in tqdm(range(len(lines1)), desc='Extracting data......'):
                temp = lines1[i]
                # print('temp = ',temp)
                if len(temp) > 50:
                    temp3 = []
                    flat_list = []
                    for j in range(len(temp)):
                        try:
                            temp2 = temp[j].split(',')
                            temp3.append(temp2)
                        except:
                            temp3.append(temp[j])
                    flat_list = [item for sublist in temp3 for item in sublist]
                    # print('ft= ',flat_list)
                    imei = flat_list[2]
                    csvwriter.writerow(flat_list)
            csvfile.close()

        csvData = pandasForSortingCSV.read_csv(parsed_file)

        # print("\nBefore sorting:")
        # print(csvData)

        csvData.sort_values(["seq_no"],
                            axis=0,
                            ascending=[True],
                            inplace=True)

        # print("\nAfter sorting:")
        # print(csvData)
        # print(type(csvData))
        csvData_reset = csvData.reset_index()
        csvData_reset.to_csv(parsed_file)
        seq = csvData_reset.seq_no

        data1 = []
        dublicate_packet = 0
        dublicate_packet1 = []
        data_loss = 0
        data_loss1 = []
        result_file = date + "_Result.csv"
        with open(result_file, 'w+', newline='') as csvfile1:
            header = ['IMEI', 'Curent_Seq_no', 'Previous_Seq_no', 'Data_loss', 'Result']
            csvwriter1 = csv.writer(csvfile1)
            csvwriter1.writerow(header)
            for x in tqdm(range(len(seq) - 1), desc='Performing Operations.......'):
                y = x + 1

                dif = seq[y] - seq[x]
                if dif > 1:
                    # print("Data loss")
                    data1.append(imei)
                    data1.append(seq[y])
                    data1.append(seq[x])
                    data1.append(dif - 1)
                    data1.append('Packet loss')
                    data_loss = data_loss + dif - 1
                    csvwriter1.writerow(data1)
                    data1 = []
                # check dublicate packet
                elif dif == 0:
                    data1.append(imei)
                    data1.append(seq[y])
                    data1.append(seq[x])
                    data1.append(dif)
                    data1.append('Dublicate Packet')
                    dublicate_packet = dublicate_packet + 1
                    csvwriter1.writerow(data1)
                    data1 = []

            data_loapss1.pend('Total_data_loss= ')
            data_loss1.append(data_loss)
            dublicate_packet1.append('Total_dublicate= ')
            dublicate_packet1.append(dublicate_packet)
            csvwriter1.writerow(data_loss1)
            csvwriter1.writerow(dublicate_packet1)
    else:
        print("Wrong server index entry")


    do = "Git push 1"

    print("-------------Completed----------")
    time.sleep(11)