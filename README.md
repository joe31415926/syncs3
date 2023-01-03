# syncs3

Put this line in the crontab

```
sudo apt -y update
sudo apt -y upgrade
sudo apt -y install awscli git
git clone https://github.com/zserge/jsmn.git
aws configure
aws s3api list-objects --bucket joeruff.com --prefix end.bin


echo `hexdump -n 3 -e '"%x"' < /dev/random`
gcc -o syncs3 syncs3.c -I jsmn/
echo "@reboot /home/pi/syncs3 /home/pi/ joeruff.com 2D4090" | crontab

```
