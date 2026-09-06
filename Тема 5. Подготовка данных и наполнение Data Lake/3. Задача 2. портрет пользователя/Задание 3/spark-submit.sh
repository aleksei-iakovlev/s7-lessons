!spark-submit --master yarn --num-executors 10 \
            --deploy-mode cluster \
            --executor-memory 12g \
            --executor-cores 12 \
            --driver-memory 6g \
            --driver-cores 6 \
            /lessons/calculate_user_interests.py 

!hdfs dfs -ls /user/s19290263/data/tmp/