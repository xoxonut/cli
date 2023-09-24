# cli
```
ip <kafka/couchdb> <ip:port> // set ip to config.json
create <kafka/couchdb> <account id>
read <kafka/couchdb> <account id>
update <kafka/couchdb> <account id> <target amount>
delete <kafka/couchdb> <account id>
txnok <kafka/couchdb> <giver id> <kafka/couchdb> <receiver id> <amount> // if ok then commit or abort
txnbad <kafka/couchdb> <giver id> <kafka/couchdb> <receiver id> <amount> // no matter how abort
reset <kafka/couchdb> //clear all db
```
