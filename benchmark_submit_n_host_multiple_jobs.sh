for k in $(seq 30); do
  sed 's/KV="KV"/KV="papyrus"/g' benchmark_throughput_only_one.sh | sed  's/report_one_host/report_n_host/g' | bsub -R "rusage[scratch=5120] span[ptile=2] select[model==EPYC_7742]"
  sed 's/KV="KV"/KV="paperless"/g' benchmark_throughput_only_one.sh | sed  's/report_one_host/report_n_host/g' | bsub -R "rusage[scratch=5120] span[ptile=2] select[model==EPYC_7742]"
  sed 's/KV="KV"/KV="Ipaperless"/g' benchmark_throughput_only_one.sh | sed  's/report_one_host/report_n_host/g' | bsub -R "rusage[scratch=5120] span[ptile=2] select[model==EPYC_7742]"
done