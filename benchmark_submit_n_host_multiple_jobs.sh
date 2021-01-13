for k in $(seq 30); do
  sed "s/KV="KV"/KV="papyrus"/g; s/report_one_host/report_n_host/g; s/st=0/st=${k}/g" benchmark_throughput_only_one.sh | bsub -R "rusage[scratch=5120] span[ptile=2] select[model==EPYC_7742]"
  sed "s/KV="KV"/KV="paperless"/g; s/report_one_host/report_n_host/g; s/st=0/st=${k}/g" benchmark_throughput_only_one.sh | bsub -R "rusage[scratch=5120] span[ptile=2] select[model==EPYC_7742]"
  sed "s/KV="KV"/KV="Ipaperless"/g; s/report_one_host/report_n_host/g; s/st=0/st=${k}/g" benchmark_throughput_only_one.sh | bsub -R "rusage[scratch=5120] span[ptile=2] select[model==EPYC_7742]"
done