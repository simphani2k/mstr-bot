#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 11 01:54:09 2022

@author: ngunn
"""

from mstrio.connection import Connection
from mstrio.project_objects.datasets import OlapCube
import time
from datetime import datetime
import pandas as pd

def connect ():
    # connect to Intelligence Server
    # Variables
    base_url = "https://lighthouse.pvh.com/LibrarySTD/api"
    mstr_username = "neilgunn@pvh.com"
    mstr_password = "w4JzVe3cYH2"
    project_id = "D75AE0B1ED4B58272AED4C83BCF329E3" #lighthouse prod
    conn = Connection(base_url, mstr_username, mstr_password, project_id=project_id)
    conn.connect()
    return conn

def cube_object (conn, cube_id):
    refresh_cube = OlapCube(conn, cube_id)
    return refresh_cube

def publish (refresh_cube):
    # get OLAP Cube by its identifier
    refresh_cube.publish()

def status(refresh_cube):
    refresh_cube.refresh_status()
    cube_state = refresh_cube.show_status()
    return cube_state

# create list of cubes in priority order
cube_list = ["2BE83BE144E41434E797A3ADBD9E87AF",
			"7B7999E5466A65E71DA8FA9A89F0B90E",
			"8B10267D40B3330090DDE899B019A685",
			"6CB44DF14C1668DF8CBE5082B0B9387D",
			"911A873140B36061D3A48AB48023B77E"
			"3B99184B4BD1C6DD8A7F688C19BB17D6",
			"144571D74DD43B91D9AC968140D19177",
			"3B5154DF4B1033AE251A57B8F7494C01",
			"396B440C425AFA71948B81A46D1810D4",
			"9ABB18054280B0DFB8415EA6FFBE005F",
			"239E4138494F616A1BF1BB97AAD5D74D",
			"E1F9F3A64FECA19B7D9BC98016C18229",
			"9AE464A944EBC5229031BD995EDFAE03",
			"4D6A39F343AE808182228783BB9D56D8"
             ]

# connect and initialize 
conn = connect()
concurrency = 4
running_jobs = 0
pub_cubes = 0
pub_list = []
stats_df = pd.DataFrame(columns=["cube_id", "cube_name", "start_time", "end_time"])
stats_df['cube_id'] = cube_list

for i in cube_list:
    pub_list.append(i)
    refresh_cube = cube_object (conn, i)
    publish(refresh_cube)
    #find row and update
    update_row = stats_df.cube_id[stats_df.cube_id == i].index.tolist()
    stats_df.loc[update_row, 'start_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    stats_df.loc[update_row, 'cube_name'] = refresh_cube.name
    print("Publishing cube -  ", refresh_cube.name)
    running_jobs = running_jobs + 1
    pub_cubes = pub_cubes + 1
    while running_jobs == concurrency or ((running_jobs > 0) & (pub_cubes == len(cube_list))):
        print ("Running Jobs reached concurrency -  ", running_jobs)
        for k in pub_list:
            refresh_cube = cube_object(conn, k)
            print('checking cube status -  ', refresh_cube.name)
            cube_state = status(refresh_cube)
            if cube_state == ['Ready', 'Loaded', 'Persisted', 'Active', 'Reserved']:
                if len(pub_list) == 1:
                    pub_list = []
                    #find row and update
                    update_row = stats_df.cube_id[stats_df.cube_id == k].index.tolist()
                    stats_df.loc[update_row, 'end_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                else: 
                    pub_list.remove(k)
                    #find row and update
                    update_row = stats_df.cube_id[stats_df.cube_id == k].index.tolist()
                    stats_df.loc[update_row, 'end_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            else: time.sleep(10)
        running_jobs = len(pub_list)
    
# capture run times and print
stats_df['start_time'] = pd.to_datetime(stats_df['start_time'])
stats_df['end_time'] = pd.to_datetime(stats_df['end_time'])
stats_df['runtime_secs'] = (stats_df['end_time'] - stats_df['start_time']).dt.total_seconds()
stats_df['runtime_mins'] = round(stats_df['runtime_secs'] / 60, 1)
print(stats_df)