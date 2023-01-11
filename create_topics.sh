topics=($(cat topics-list.txt))

for topic_name in "${topics[@]}"
do
    :
     docker exec -it broker /bin/kafka-topics --bootstrap-server localhost:9092 --topic "$topic_name" --create --if-not-exists --partitions 3
done
