#!/usr/bin/env bash

# 数据库连接信息
user='cloud'
passwd='aopfojdasfk235szdf'
port='3307'
host='127.0.0.1'
db='cloud'
table='speed5m'

# 显示使用方法
function show_usage() {
    echo "用法: $0 <edc> [month] [year]"
    echo "  edc: 数据中心名称，支持通配符匹配(如 BJ-jinshan-* 可同时匹配并累加 BJ-jinshan-01、BJ-jinshan-02 等)"
    echo "  month: 月份(1-12)，不指定则导出全年数据"
    echo "  year: 年份，不指定则使用当前年份"
    exit 1
}

# 参数检查
if [ -z "$1" ]; then
    show_usage
fi

edc=$1
month=$2
year=${3:-$(date +'%Y')} # 如果未指定年份，使用当前年份

# 查询单月数据
function query_month_data() {
    local month=$1
    local output_file=$2
    local separator=$3
    
    echo "处理 ${year}-${month} 月数据..."
    
    # 如果需要分隔符且不是第一个月，添加分隔符
    if [ "$separator" = "true" ] && [ -s "$output_file" ]; then
        echo "---" >> "$output_file"
    fi
    
    # 添加月份标题
    echo "## ${year}年${month}月" >> "$output_file"
    
    # 去除月份前导零以便于比较
    month_no_leading_zero=$(echo $month | sed 's/^0*//')
    
    case $month_no_leading_zero in
        2)
            # 检查是否为闰年
            if (( year % 400 == 0 || ( year % 4 == 0 && year % 100 != 0 ) )); then
                days=29
            else
                days=28
            fi
            ;;
        4|6|9|11)
            days=30
            ;;
        1|3|5|7|8|10|12)
            days=31
            ;;
        *)
            echo "不支持的月份: $month"
            return 1
    esac
    
    for ((i=1; i<=days; i++))
    do
        # 格式化日期，确保个位数日期前面有0
        day=$(printf "%02d" $i)
        
        # 处理edc通配符，将*转换为SQL的%
        edc_pattern=$(echo "$edc" | sed 's/\*/%/g')
        
        # 使用REGEXP或LIKE根据是否包含通配符
        if [[ "$edc" == *"*"* ]]; then
            # 包含通配符，使用转换后的模式
            mysql -u${user} -P${port} -h${host} -p${passwd} ${db} -e "select create_time,sum(service_size) as total_service_size from traffic_5m where edc_name like '${edc_pattern}' and edc_name not like '%-backup' and create_time >= '${year}-${month}-${day} 00:00:00' and create_time < '${year}-${month}-${day} 23:59:59' group by create_time order by total_service_size desc limit 14,1" >> "$output_file"
        else
            # 不包含通配符，使用原来的精确匹配方式
            mysql -u${user} -P${port} -h${host} -p${passwd} ${db} -e "select create_time,sum(service_size) as total_service_size from traffic_5m where edc_name like '${edc}%' and edc_name not like '%-backup' and create_time >= '${year}-${month}-${day} 00:00:00' and create_time < '${year}-${month}-${day} 23:59:59' group by create_time order by total_service_size desc limit 14,1" >> "$output_file"
        fi
    done
    
    # 清理重复列
    awk '!seen[$1]++' "$output_file" > day95.tmp
    cat day95.tmp > "$output_file"
    rm -f day95.tmp
}

# 主逻辑
if [ -z "$month" ]; then
    # 导出全年数据
    # 处理输出文件名，如果edc包含通配符，替换为适合文件名的表示
    safe_edc=$(echo "$edc" | sed 's/\*/_ALL_/g')
    output_file="${safe_edc}_${year}_全年.txt"
    echo "" > "$output_file"
    
    for ((m=1; m<=12; m++))
    do
        # 格式化月份，确保个位数月份前面有0
        month_formatted=$(printf "%02d" $m)
        query_month_data "$month_formatted" "$output_file" "true"
    done
    
    echo "全年数据已导出到 ${output_file}"
else
    # 导出单月数据
    # 处理输出文件名，如果edc包含通配符，替换为适合文件名的表示
    safe_edc=$(echo "$edc" | sed 's/\*/_ALL_/g')
    output_file="${safe_edc}_${year}_${month}.txt"
    echo "" > "$output_file"
    
    # 格式化月份，确保个位数月份前面有0
    month_formatted=$(printf "%02d" $month)
    query_month_data "$month_formatted" "$output_file" "false"
    
    echo "${month}月数据已导出到 ${output_file}"
fi