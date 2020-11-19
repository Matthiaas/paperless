for i in 1 2 4
do
  for ((j=0; j <$i; j++))
  do
    mkdir /tmp/PaperlessTest${j}
  done
  mpiexec -n $i ./build/mpi_tests [${i}rank]
  for ((j=0; j <$i; j++))
  do
    rm -r /tmp/PaperlessTest${j}
  done

done