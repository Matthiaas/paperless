bsub < artificial_workload_benchmark.sh
bsub < seq_vs_rel_benchmark.sh
bsub < throughtput.sh

sed  's/report_one_host/report_n_host/g' artificial_workload_benchmark.sh | bsub -R "rusage[scratch=5120] span[ptile=2]"
sed  's/report_one_host/report_n_host/g' seq_vs_rel_benchmark.sh | bsub -R "rusage[scratch=5120] span[hosts=2]"
sed  's/report_one_host/report_n_host/g' throughtput.sh |  bsub -R "rusage[scratch=5120] span[hosts=2]"