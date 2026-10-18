docker build -t hawkeye .

### to login and create the tokencache (serde_json needs to read a valid json):
`echo '[]' tokencache.json`
`docker run -it --name hawkeye --volume=./clientsecret.json:/clientsecret.json --volume=./tokencache.json:/tokencache.json --volume=.env:/.env hawkeye`

Ctrl-p + Ctrl-q to quit docker tty


### frigate http api
curl -v -L 'http://192.168.300.300:8971/api/login' \
                                     -H 'Content-Type: application/json' \
                                     -H 'Accept: application/json' \
                                     -d '{
                                   "user": "admin",
                                   "password": "password"
                                 }'

export TOKEN="eyJ0elonglongtoken.longtoken_longlongtokenUCpYTxG4s"

curl -v --output imgS.jpg 'http://192.168.300.300:8971/api/events/1784215015.268634-jis739/snapshot.jpg' -H "Authorization: Bearer $TOKEN"
