#!/usr/bin/env bash

user='cloud'
passwd='aopfojdasfk235szdf'
port='3307'
host='127.0.0.1'
db='cloud'
table='speed5m'
edc=$1
month=$2
year=$(date +'%Y')
#year=2024
output=${edc}_${year}_${2}_max

echo '' > ${output}.txt

case $2 in
    2|4|6|9|11)
        for ((i=1;i<=30;i++))
        do
        mysql -u${user} -P${port} -h${host} -p${passwd} ${db} -e "select create_time,sum(service_size) as total_service_size from traffic_5m where edc_name like '${edc}%' and edc_name not like '%-backup' and create_time >= '${year}-${2}-${i} 00:00:00' and create_time < '${year}-${2}-${i} 23:59:59' group by create_time order by total_service_size desc limit 1" >> ${output}.txt
        done
        ;;
    1|3|5|7|8|10|12)
        for ((i=1;i<=31;i++))
        do
        mysql -u${user} -P${port} -h${host} -p${passwd} ${db} -e "select create_time,sum(service_size) as total_service_size from traffic_5m where edc_name like '${edc}%' and edc_name not like '%-backup' and create_time >= '${year}-${2}-${i} 00:00:00' and create_time < '${year}-${2}-${i} 23:59:59' group by create_time order by total_service_size desc limit 1" >> ${output}.txt
        done
        ;;
    *)
        echo "unsupport case"
        exit 1
esac

# clear the duplicate column
awk '!seen[$1]++' ${output}.txt > day95.tmp
cat day95.tmp > ${output}.txt
rm -f day95.tmp