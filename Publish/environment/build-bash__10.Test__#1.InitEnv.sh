set -e


#---------------------------------------------------------------------
# args

args_="

export basePath=/root/temp

# "


#---------------------------------------------------------------------
echo '#build-bash__10.Test__#1.InitEnv.sh -> #1 start ElasticSearch container'
docker rm vitorm-elasticsearch -f || true
docker run -d \
--name vitorm-elasticsearch \
-p 9200:9200 -p 9300:9300 \
-e "discovery.type=single-node" \
elasticsearch:7.10.1


#---------------------------------------------------------------------
echo '#build-bash__10.Test__#1.InitEnv.sh -> #8 wait for containers to init'


echo '#build-bash__10.Test__#1.InitEnv.sh -> #8.1 wait for ElasticSearch to init' 
docker run -t --rm --link vitorm-elasticsearch curlimages/curl timeout 120 sh -c 'until curl "http://vitorm-elasticsearch:9200"; do echo waiting for ElasticSearch; sleep 2; done;'


#---------------------------------------------------------------------
echo '#build-bash__10.Test__#1.InitEnv.sh -> #9 init test environment success!'