# for i in {1..1}
for i in 18
# for i in 1 3 6 9 12 18 24 35 48 60
do
    mkdir -p logs/btree_${i}
    ./scripts/set_uncore_frequency.sh 800000
    # /mydata/LSM-vs-BTREE/build/btree $i a.csv > logs/btree_${i}/a.log 2>&1
    # /mydata/LSM-vs-BTREE/build/btree $i b.csv > logs/btree_${i}/b.log 2>&1
    /mydata/LSM-vs-BTREE/build/btree $i c.csv > logs/btree_${i}/c.log 2>&1
    ./scripts/set_uncore_frequency.sh
done