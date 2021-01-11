bsub < benchmark_op_times.sh
bsub < benchmark_seq_vs_rel.sh
bsub < benchmark_throughput.sh
bsub < benchmark_throughput_storage.sh
sed  's/report_one_host/report_n_host/g' benchmark_op_times.sh | bsub -R "rusage[scratch=5120] span[ptile=2] select[model==EPYC_7742]"
sed  's/report_one_host/report_n_host/g' benchmark_seq_vs_rel.sh | bsub -R "rusage[scratch=5120] span[hosts=2] select[model==EPYC_7742]"
sed  's/report_one_host/report_n_host/g' benchmark_throughput.sh |  bsub -R "rusage[scratch=5120] span[hosts=2] select[model==EPYC_7742]"