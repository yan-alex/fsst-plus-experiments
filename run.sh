timestamp=$(date +"%Y-%m-%d_%H-%M-%S")
logfile="benchmarking/results/output-$timestamp.log"
nohup /export/scratch2/home/yla/fsst-plus-experiments/build/fsst_plus > "$logfile" 2>&1 & disown
tail -f "$logfile"
