# Use official Confluent Kafka Connect as base
FROM confluentinc/cp-kafka-connect:7.8.0

COPY kafka-elasticsearch-plugin /usr/share/confluent-hub-components/

# Ensure plugin path includes the installed connector
ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"

# Expose Kafka Connect REST port
EXPOSE 8083
