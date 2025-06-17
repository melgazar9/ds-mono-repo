# command to watch the cpu temp -->

watch -n 1 'sensors | grep -E "(Tctl|Tccd)"'
