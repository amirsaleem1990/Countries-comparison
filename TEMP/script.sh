#!/bin/bash
#exit

del -rf /data/old       ; mkdir /data/old
del -rf /data/dbpush    ; mkdir /data/dbpush 
del -rf /data/History   ; mkdir /data/History
del -rf logs/*
del -rf /data/temporary ; mkdir /data/temporary
./server23_amir_version_etl_process.py 1
