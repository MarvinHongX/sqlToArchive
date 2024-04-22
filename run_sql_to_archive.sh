#!/bin/bash
process_count=$(ps aux | grep "sql_to_archive.py" | grep -v "grep" | wc -l)

if [ $process_count -gt 0 ]; then
    echo "sql_to_archive.py is already running."
    exit
fi

python3 /root/sh/sqlToArchive/sql_to_archive.py  >> /root/log/sql_to_archive.log 2>&1 &
