pushd ..
source setup/setup.sh
popd

# Since we need only one file from e-mission server, let's do a hacky modularization rn
curl https://raw.githubusercontent.com/e-mission/e-mission-server/master/emission/net/ext_service/routing/osrm.py -o osrm.py
mkdir -p conf/net/ext_service
curl https://raw.githubusercontent.com/e-mission/e-mission-server/master/conf/net/ext_service/osrm.json.sample -o conf/net/ext_service/osrm.json.sample
