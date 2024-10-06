Istruzioni per eseguire progetto su istanza EC2:
1. `sudo yum install docker -y` per installare docker

Copy the appropriate docker-compose binary from GitHub.
NOTE: to get the latest version: 

2. `sudo curl -L https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m) -o /usr/local/bin/docker-compose`

Fix permissions after download:

3. `sudo chmod +x /usr/local/bin/docker-compose`

Verify success:

4. `docker-compose version`
5. `sudo yum install git -y`
6. `git clone https://github.com/matteopallagrosi/dbService.git`
7. `cd dbService`
8. `sudo service docker start`
9. `sudo docker-compose up --build`