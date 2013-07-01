#! /bin/bash

while [[ 1 ]]
do
    python voa_loader.py
    7z a voa.7z voa_archive
    mv voa.7z http/
    sleep 3600
done
