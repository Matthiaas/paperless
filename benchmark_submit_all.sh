# Run stuff on one node.
bsub < benchmark_op_times.sh
bsub < benchmark_seq_vs_rel.sh
bsub < benchmark_throughput.sh


# Run stuff on n nodes
sed  's/report_one_host/report_n_host/g' benchmark_op_times.sh | bsub -R "rusage[scratch=5120] span[ptile=2] select[model==EPYC_7742]"
sed  's/report_one_host/report_n_host/g' benchmark_seq_vs_rel.sh | bsub -R "rusage[scratch=5120] span[hosts=2] select[model==EPYC_7742]"
sed  's/report_one_host/report_n_host/g' benchmark_throughput.sh |  bsub -R "rusage[scratch=5120] span[hosts=2] select[model==EPYC_7742] rusage[mem=5120]"
# this is n hosts by default
bsub < benchmark_throughput_storage.sh


# Papyrus RELAXED vs SEQUENTIAL
sed 's/KV="paperless"/KV="papyrus"/g' benchmark_seq_vs_rel.sh | bsub
sed 's/KV="paperless"/KV="papyrus"/g' benchmark_seq_vs_rel.sh | sed  's/report_one_host/report_n_host/g' | bsub -R "rusage[scratch=5120] span[hosts=2] select[model==EPYC_7742]"