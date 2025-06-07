#!/usr/bin/bash

echo "ansible provisioning..."
echo "--------------------------------------"
bash /box/provision/ansible.sh

echo

echo "crond..."
echo "--------------------------------------"
test -x /usr/sbin/crond && /usr/sbin/crond

echo

echo "starting Apache web server..."
echo "--------------------------------------"
/usr/libexec/httpd-ssl-gencerts
exec /usr/sbin/httpd -D FOREGROUND &
wait

