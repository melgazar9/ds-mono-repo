# Reduce CPU frequency when running long CPU intensive jobs

for cpu in /sys/devices/system/cpu/cpu[0-9]*; do
  sudo cpufreq-set -c $(basename $cpu | grep -o '[0-9]\+') -d 3.9GHz -u 4.8GHz
done
