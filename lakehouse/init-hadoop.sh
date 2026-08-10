#!/bin/bash
# Attendre que le namenode soit prêt
until hdfs dfsadmin -report 2>/dev/null; do
  echo "Waiting for namenode..."
  sleep 5
done

# Créer les répertoires HDFS nécessaires
hadoop fs -mkdir -p /spark-jars
hadoop fs -mkdir -p /tmp
hadoop fs -mkdir -p /user/hive/warehouse

# Définir les permissions (correction du chmod)
hadoop fs -chmod -R 777 /tmp
hadoop fs -chmod -R 777 /user/hive/warehouse

# Copier les JARs (avec vérification des chemins)
HADOOP_LIBS=(
  "/opt/hadoop/share/hadoop/common/*.jar"
  "/opt/hadoop/share/hadoop/hdfs/*.jar"
  "/opt/hadoop/share/hadoop/yarn/*.jar"
  "/opt/hadoop/share/hadoop/mapreduce/*.jar"
)

for lib in "${HADOOP_LIBS[@]}"; do
  if ls $lib >/dev/null 2>&1; then
    hadoop fs -put $lib /spark-jars/
  else
    echo "Warning: Library files not found - $lib"
  fi
done

echo "HDFS initialization completed successfully"