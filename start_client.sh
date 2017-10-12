#!/bin/bash
python client.py --master-hostname sscloud11 --master-port 5673 --thread-number 150 --decode-path /cephfs2/mr_decode/ --unzip-path /mem_swap/ --client-name $1
