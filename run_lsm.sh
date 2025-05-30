# for i in {1..1}
# for i in 18
for i in 1 3 6 9 12 18 24 35 48 60
do
    mkdir -p logs/lsm_${i}
    # ./scripts/set_uncore_frequency.sh 800000
    # gdb --args /mydata/LSM-vs-BTREE/build/lsm $i a.csv
    /mydata/LSM-vs-BTREE/build/lsm $i a.csv > logs/lsm_${i}/a.log 2>&1
    /mydata/LSM-vs-BTREE/build/lsm $i b.csv > logs/lsm_${i}/b.log 2>&1
    /mydata/LSM-vs-BTREE/build/lsm $i c.csv > logs/lsm_${i}/c.log 2>&1
    # ./scripts/set_uncore_frequency.sh
done 