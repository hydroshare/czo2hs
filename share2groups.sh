echo "Copying scripts to hydroshare container"
docker cp ./share2groups.py hydroshare:/tmp/share2groups.py
docker cp ./settings.py hydroshare:/tmp/settings.py
echo "Running script"
docker exec -it -u hydro-service hydroshare bash -c "cd /tmp; python /hydroshare/manage.py shell < share2groups.py"
echo "Done"