ps -ef | grep -i kafka | awk '{print $2}' | xargs kill -9