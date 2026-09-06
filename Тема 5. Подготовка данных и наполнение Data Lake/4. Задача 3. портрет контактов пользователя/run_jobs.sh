spark-submit --master yarn --num-executors 10 \
    --deploy-mode cluster \
    --executor-memory 6g \
    --executor-cores 6 \
    --driver-memory 6g \
    --driver-cores 6 \
    /lessons/connection_interests.py \
    2022-05-25 7 \
    /user/s19290263/data/events \
    /user/s19290263/analytics \
    /user/s19290263/data/analytics