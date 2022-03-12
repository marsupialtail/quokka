import os
import time
import boto3

def check_instance_alive(public_ip):
    z = os.system("ssh -oStrictHostKeyChecking=no -oConnectTimeout=2 -i /home/ziheng/Downloads/oregon-neurodb.pem ubuntu@" + public_ip)
    if z == 0:
        return False
    else:
        return True

def launch_new_instance():
    ec2 = boto3.client("ec2")
    waiter = ec2.get_waiter('instance_running')

    # important 2 things:
    # this instance needs to have all the things installed on it
    # this instance needs to have the right tcp permissions
    res = ec2.run_instances(ImageId="ami-033cb7cdd46905a76", InstanceType = "c5.2xlarge", SecurityGroupIds = ["sg-0770c1101ab26fba2"], KeyName="oregon-neurodb",MaxCount=1, MinCount=1)
    instance_ids = res['Instances'][0]['InstanceId'] 
    start_time = time.time()
    waiter.wait(InstanceIds=[instance_ids])
    print("Relaunch of EC2 instance used: ", time.time() - start_time)
    public_ip = ec2.describe_instances(InstanceIds = [instance_ids])['Reservations'][0]['Instances'][0]['PublicIpAddress']
    private_ip = ec2.describe_instances(InstanceIds = [instance_ids])['Reservations'][0]['Instances'][0]['PrivateIpAddress']

    count = 0
    while True:
        z = os.system("ssh -oStrictHostKeyChecking=no -oConnectTimeout=2 -i /home/ziheng/Downloads/oregon-neurodb.pem ubuntu@" + public_ip)
        if z == 0:
            break
        else:
            count += 1
            if count == 6:
                raise Exception("Couldn't connect to new instance in 30 seconds.")
            time.sleep(5)

    z = os.system("ssh -oStrictHostKeyChecking=no -i /home/ziheng/Downloads/oregon-neurodb.pem ubuntu@" + public_ip + 
    "ray start --address='172.31.48.233:6379' --redis-password='5241590000000000'")
    if z != 0:
        raise Exception("Failed to start the new EC2 worker")

    z = os.system("ssh -oStrictHostKeyChecking=no -i /home/ziheng/Downloads/oregon-neurodb.pem ubuntu@" + public_ip + 
    "redis-6.2.6/src/redis-server redis-6.2.6/redis.conf --port 6800 --protected-mode no&")
    if z!= 0:
        raise Exception("Failed to start Redis server on new worker")
    return private_ip