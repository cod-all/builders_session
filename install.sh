sudo mv mongodb-org-4.0.repo /etc/yum.repos.d/
sudo pip3 install -r requirements.txt
sudo yum install -y mongodb-org
export PATH=$PATH:/usr/local/bin