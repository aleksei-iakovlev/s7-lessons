!/usr/lib/spark/bin/spark-submit --master yarn --num-executors 10 --deploy-mode cluster \
    --executor-memory 10g \
    --executor-cores 10 \
    --driver-memory 4g \
    --driver-cores 4 \
    /lessons/user_interests.py\
    2022-05-31 28 /user/s19290263/data/events /user/s19290263/analytics/