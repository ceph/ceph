#!/usr/bin/bash
# single FIO instance
for x in *_1procs_*.zip; do 
	unzip -d tempo $x; 
	cd tempo; 
	z=${x/.zip}; y=${x/.zip/_json.out}; lst=${x/.zip/_list}; avg=${x/.zip/_cpu_avg.json};
	python3 /root/bin/fio-parse-jsons.py -c $lst -t $z -a $avg > $y;
	for xx in *.plot; do gnuplot $xx; done
	zip -9umq ../$x *;
       	cd -; 
done

# multiple FIO
for x in *_8procs_*.zip; do 
	unzip -d tempo $x; 
	cd tempo; 
	z=${x/.zip}; y=${x/.zip/_json.out}; lst=${x/.zip/_list}; avg=${x/.zip/_cpu_avg.json};
	python3 /root/bin/fio-parse-jsons.py -m -c $lst -t $z -a $avg > $y;
	for xx in *.plot; do gnuplot $xx; done
	zip -9umq ../$x *;
       	cd -; 
done
