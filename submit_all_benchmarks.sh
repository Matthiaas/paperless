bsubs < artificial_workload_benchmark.sh
bsubs < seq_vs_rel_benchmark.sh
bsubs < throughtput.sh

sed  's/report_one_host/report_n_host/g' artificial_workload_benchmark.sh | bsubs -R "rusage[scratch=5120] span[ptile=2]"
sed  's/report_one_host/report_n_host/g' seq_vs_rel_benchmark.sh | bsubs -R "rusage[scratch=5120] span[hosts=2]"
sed  's/report_one_host/report_n_host/g' throughtput.sh |  bsubs -R "rusage[scratch=5120] span[hosts=2]"