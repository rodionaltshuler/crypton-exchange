docker exec -it broker /bin/kafka-streams-application-reset --application-id orders-processing-application-stream
docker exec -it broker /bin/kafka-streams-application-reset --application-id api-order-stream
