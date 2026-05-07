#!/bin/bash
set -euo pipefail

# ---------- Configuration ----------
readonly APP_NAME="lmd"
readonly INTERFACE="eth0"
readonly INTERVAL=1

# ---------- Global Peaks & Averages ----------
peak_app_cpu=0
peak_app_mem_pct=0
peak_app_mem_kb=0
peak_sshd_cpu=0
peak_sshd_mem_pct=0
peak_sshd_mem_kb=0
peak_ssh_cpu=0
peak_ssh_mem_pct=0
peak_ssh_mem_kb=0
peak_tx_mbps=0
peak_rx_mbps=0

# Averages (accumulated sums and counters)
total_app_cpu=0
total_sshd_cpu=0
total_ssh_cpu=0
sample_count=0

# ---------- Constants ----------
readonly TOTAL_MEM_KB=$(awk '/MemTotal/ {print $2}' /proc/meminfo)
readonly TOTAL_JIFFIES_PER_SEC=$(getconf CLK_TCK 2>/dev/null || echo 100)
readonly PAGE_SIZE_KB=$(getconf PAGE_SIZE 2>/dev/null | awk '{print $1/1024}')
readonly PAGE_SIZE_KB=${PAGE_SIZE_KB:-4}   # fallback to 4KB

# Validate network interface
if [[ ! -d "/sys/class/net/$INTERFACE" ]]; then
    echo "ERROR: Interface '$INTERFACE' does not exist." >&2
    exit 1
fi

# ---------- Helper Functions ----------
get_pids_exact() {
    pgrep -x "$1" 2>/dev/null || true
}

# Returns total jiffies (utime+stime) for a PID, and starttime (in jiffies since boot)
get_proc_cpu_and_start() {
    local pid=$1
    local stat_file="/proc/$pid/stat"
    if [[ -f "$stat_file" ]]; then
        # Fields: 14=utime, 15=stime, 22=starttime
        awk '{print $14+$15, $22}' "$stat_file"
    else
        echo "0 0"
    fi
}

# Returns RSS in KiB using page size
get_mem_kb() {
    local pid=$1
    local statm_file="/proc/$pid/statm"
    if [[ -f "$statm_file" ]]; then
        awk -v ps="$PAGE_SIZE_KB" '{printf "%.0f", $2 * ps}' "$statm_file"
    else
        echo 0
    fi
}

# ---------- Pre‑run Setup ----------
echo "Monitoring $APP_NAME, sshd, ssh. Interval=${INTERVAL}s. Interface=$INTERFACE"
echo "Press Ctrl+C to stop and see final report."

# Network initial sample
tx_before=$(cat "/sys/class/net/$INTERFACE/statistics/tx_bytes" 2>/dev/null || echo 0)
rx_before=$(cat "/sys/class/net/$INTERFACE/statistics/rx_bytes" 2>/dev/null || echo 0)

# Associative arrays: store previous jiffies AND starttime
declare -A prev_app_jiffies prev_app_start
declare -A prev_sshd_jiffies prev_sshd_start
declare -A prev_ssh_jiffies prev_ssh_start

# ---------- Trap for Final Report ----------
finish() {
    echo -e "\n\n--- FINAL PEAK REPORT ---"
    echo "Peak App CPU: ${peak_app_cpu}%"
    echo "Peak App RAM: ${peak_app_mem_pct}% (${peak_app_mem_kb} KB)"
    echo "--------------------------------"
    echo "Peak SshD CPU: ${peak_sshd_cpu}%"
    echo "Peak SshD RAM: ${peak_sshd_mem_pct}% (${peak_sshd_mem_kb} KB)"
    echo "--------------------------------"
    echo "Peak Ssh CPU: ${peak_ssh_cpu}%"
    echo "Peak Ssh RAM: ${peak_ssh_mem_pct}% (${peak_ssh_mem_kb} KB)"
    echo "--------------------------------"
    echo "Peak TX: ${peak_tx_mbps} Mbps"
    echo "Peak RX: ${peak_rx_mbps} Mbps"
    echo -e "\n--- AVERAGE CPU % (over ${sample_count} samples) ---"
    if [[ $sample_count -gt 0 ]]; then
        avg_app=$(awk -v s="$total_app_cpu" -v c="$sample_count" 'BEGIN {printf "%.1f", s/c}')
        avg_sshd=$(awk -v s="$total_sshd_cpu" -v c="$sample_count" 'BEGIN {printf "%.1f", s/c}')
        avg_ssh=$(awk -v s="$total_ssh_cpu" -v c="$sample_count" 'BEGIN {printf "%.1f", s/c}')
        echo "App:  ${avg_app}%"
        echo "SshD: ${avg_sshd}%"
        echo "Ssh:  ${avg_ssh}%"
    else
        echo "(no samples collected)"
    fi
}
trap finish EXIT

# ---------- Main Monitoring Loop ----------
while true; do
    sleep "$INTERVAL"

    # --- Network stats (delta) ---
    tx_after=$(cat "/sys/class/net/$INTERFACE/statistics/tx_bytes" 2>/dev/null || echo 0)
    rx_after=$(cat "/sys/class/net/$INTERFACE/statistics/rx_bytes" 2>/dev/null || echo 0)

    curr_tx_bytes=0
    curr_rx_bytes=0
    [[ "$tx_after" =~ ^[0-9]+$ && "$tx_before" =~ ^[0-9]+$ && "$tx_after" -ge "$tx_before" ]] && curr_tx_bytes=$((tx_after - tx_before))
    [[ "$rx_after" =~ ^[0-9]+$ && "$rx_before" =~ ^[0-9]+$ && "$rx_after" -ge "$rx_before" ]] && curr_rx_bytes=$((rx_after - rx_before))

    # Convert to Megabits per second (10^6 bits/s)
    curr_tx_mbps=$(awk -v b="$curr_tx_bytes" -v i="$INTERVAL" 'BEGIN {printf "%.2f", (b * 8) / 1000000 / i}')
    curr_rx_mbps=$(awk -v b="$curr_rx_bytes" -v i="$INTERVAL" 'BEGIN {printf "%.2f", (b * 8) / 1000000 / i}')

    tx_before=$tx_after
    rx_before=$rx_after

    # --- Helper: process group CPU & memory ---
    compute_group_stats() {
        local pids_name=$1   # "app", "sshd", or "ssh"
        local -n pids_arr=$2 # nameref to array of PIDs
        local -n cpu_sum=$3  # output: total CPU% for this interval
        local -n mem_total=$4 # output: total RSS in KB
        local -n mem_pct=$5   # output: memory percentage
        local -n mem_mb=$6     # output: memory in MB

        cpu_sum=0
        mem_total=0
        local -n prev_jiffies="prev_${pids_name}_jiffies"
        local -n prev_start="prev_${pids_name}_start"

        for pid in "${pids_arr[@]}"; do
            [[ -z "$pid" ]] && continue
            read curr_jiffies curr_start < <(get_proc_cpu_and_start "$pid")
            [[ -z "$curr_jiffies" || "$curr_jiffies" -eq 0 ]] && continue

            prev_jiff="${prev_jiffies[$pid]:-}"
            prev_st="${prev_start[$pid]:-}"

            # Only compute delta if PID existed before AND start time matches (no reuse)
            if [[ -n "$prev_jiff" && -n "$prev_st" && "$curr_start" == "$prev_st" && "$curr_jiffies" -ge "$prev_jiff" ]]; then
                delta_jiff=$((curr_jiffies - prev_jiff))
                cpu_pct=$(awk -v dj="$delta_jiff" -v i="$INTERVAL" -v jps="$TOTAL_JIFFIES_PER_SEC" \
                    'BEGIN {printf "%.1f", (dj / (i * jps)) * 100}')
                cpu_sum=$(awk -v s="$cpu_sum" -v p="$cpu_pct" 'BEGIN {printf "%.1f", s + p}')
            fi

            # Store current values
            prev_jiffies[$pid]=$curr_jiffies
            prev_start[$pid]=$curr_start

            mem_kb=$(get_mem_kb "$pid")
            mem_total=$((mem_total + mem_kb))
        done

        if [[ "$TOTAL_MEM_KB" -gt 0 ]]; then
            mem_pct=$(awk -v kb="$mem_total" -v total="$TOTAL_MEM_KB" 'BEGIN {printf "%.1f", (kb / total) * 100}')
            mem_mb=$(awk -v kb="$mem_total" 'BEGIN {printf "%.1f", kb / 1024}')
        else
            mem_pct="0.0"
            mem_mb="0.0"
        fi
    }

    # Get current PIDs for each group
    app_pids=($(get_pids_exact "$APP_NAME"))
    sshd_pids=($(get_pids_exact "sshd"))
    ssh_pids=($(get_pids_exact "ssh"))

    # Compute stats for each group
    compute_group_stats "app" app_pids app_cpu_sum app_mem_kb_total app_mem_pct app_mem_mb
    compute_group_stats "sshd" sshd_pids sshd_cpu_sum sshd_mem_kb_total sshd_mem_pct sshd_mem_mb
    compute_group_stats "ssh" ssh_pids ssh_cpu_sum ssh_mem_kb_total ssh_mem_pct ssh_mem_mb

    # Update peaks
    if awk "BEGIN {exit !($app_cpu_sum > $peak_app_cpu)}"; then peak_app_cpu=$app_cpu_sum; fi
    if awk "BEGIN {exit !($app_mem_pct > $peak_app_mem_pct)}"; then peak_app_mem_pct=$app_mem_pct; peak_app_mem_kb=$app_mem_kb_total; fi
    if awk "BEGIN {exit !($sshd_cpu_sum > $peak_sshd_cpu)}"; then peak_sshd_cpu=$sshd_cpu_sum; fi
    if awk "BEGIN {exit !($sshd_mem_pct > $peak_sshd_mem_pct)}"; then peak_sshd_mem_pct=$sshd_mem_pct; peak_sshd_mem_kb=$sshd_mem_kb_total; fi
    if awk "BEGIN {exit !($ssh_cpu_sum > $peak_ssh_cpu)}"; then peak_ssh_cpu=$ssh_cpu_sum; fi
    if awk "BEGIN {exit !($ssh_mem_pct > $peak_ssh_mem_pct)}"; then peak_ssh_mem_pct=$ssh_mem_pct; peak_ssh_mem_kb=$ssh_mem_kb_total; fi
    if awk "BEGIN {exit !($curr_tx_mbps > $peak_tx_mbps)}"; then peak_tx_mbps=$curr_tx_mbps; fi
    if awk "BEGIN {exit !($curr_rx_mbps > $peak_rx_mbps)}"; then peak_rx_mbps=$curr_rx_mbps; fi

    # Update averages (sum over samples)
    total_app_cpu=$(awk -v s="$total_app_cpu" -v c="$app_cpu_sum" 'BEGIN {printf "%.1f", s + c}')
    total_sshd_cpu=$(awk -v s="$total_sshd_cpu" -v c="$sshd_cpu_sum" 'BEGIN {printf "%.1f", s + c}')
    total_ssh_cpu=$(awk -v s="$total_ssh_cpu" -v c="$ssh_cpu_sum" 'BEGIN {printf "%.1f", s + c}')
    ((sample_count++))

    # Live display (overwrites same line)
    printf "\rApp: CPU %s%% | Mem %s%% (%s MB) | SshD: CPU %s%% | Mem %s%% (%s MB) | Ssh: CPU %s%% | Mem %s%% (%s MB) | Net: TX %s Mbps | RX %s Mbps" \
        "$app_cpu_sum" "$app_mem_pct" "$app_mem_mb" \
        "$sshd_cpu_sum" "$sshd_mem_pct" "$sshd_mem_mb" \
        "$ssh_cpu_sum" "$ssh_mem_pct" "$ssh_mem_mb" \
        "$curr_tx_mbps" "$curr_rx_mbps"
done
