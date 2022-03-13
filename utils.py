import os
import time
import boto3

def check_instance_alive(public_ip):
    z = os.system("ssh -oStrictHostKeyChecking=no -oConnectTimeout=2 -i /home/ziheng/Downloads/oregon-neurodb.pem ubuntu@" + public_ip)
    if z == 0:
        return False
    else:
        return True

def launch_new_instances(aws_access_key, aws_access_id, num_instances = 1, instance_type = "i3.2xlarge"):
    ec2 = boto3.client("ec2")
    waiter = ec2.get_waiter('instance_running')

    # important 2 things:
    # this instance needs to have all the things installed on it
    # this instance needs to have the right tcp permissions
    res = ec2.run_instances(ImageId="ami-0ac46f512c1730e1a", InstanceType = instance_type, SecurityGroupIds = ["sg-0770c1101ab26fba2"], KeyName="oregon-neurodb",MaxCount=num_instances, MinCount=num_instances)
    instance_ids = [res['Instances'][i]['InstanceId'] for i in range(num_instances)] 
    start_time = time.time()
    waiter.wait(InstanceIds=instance_ids)
    a = ec2.describe_instances(InstanceIds = instance_ids)
    public_ips = [a['Reservations'][0]['Instances'][i]['PublicIpAddress'] for i in range(num_instances)]
    private_ips = [a['Reservations'][0]['Instances'][i]['PrivateIpAddress'] for i in range(num_instances)]

    count = 0
    while True:
        z = [os.system("ssh -oStrictHostKeyChecking=no -oConnectTimeout=2 -i /home/ziheng/Downloads/oregon-neurodb.pem ubuntu@" + public_ip +" time") for public_ip in public_ips]
        if sum(z) == 0:
            break
        else:
            count += 1
            if count == 6:
                raise Exception("Couldn't connect to new instance in 30 seconds.")
            time.sleep(5)
    
    print("Launching of EC2 on-demand instances used: ", time.time() - start_time)

    z = [os.system("ssh -oStrictHostKeyChecking=no -i /home/ziheng/Downloads/oregon-neurodb.pem ubuntu@" + public_ip + 
    " aws configure set aws_secret_access_key " + str(aws_access_key)) for public_ip in public_ips]
    if sum(z) != 0:
        raise Exception("Failed to set AWS access key")
    z = [os.system("ssh -oStrictHostKeyChecking=no -i /home/ziheng/Downloads/oregon-neurodb.pem ubuntu@" + public_ip + 
    " aws configure set aws_access_key_id " + str(aws_access_id)) for public_ip in public_ips]
    if sum(z) != 0:
        raise Exception("Failed to set AWS access id")

    #z = os.system("ssh -oStrictHostKeyChecking=no -i /home/ziheng/Downloads/oregon-neurodb.pem ubuntu@" + public_ip + 
    #"ray start --address='172.31.48.233:6379' --redis-password='5241590000000000'")
    #if z != 0:
    #    raise Exception("Failed to start the new EC2 worker")

    z = [os.system("ssh -oStrictHostKeyChecking=no -i /home/ziheng/Downloads/oregon-neurodb.pem ubuntu@" + public_ip + 
    " redis-6.2.6/src/redis-server redis-6.2.6/redis.conf --port 6800 --protected-mode no&") for public_ip in public_ips]
    if sum(z)!= 0:
        raise Exception("Failed to start Redis server on new worker")
    return public_ips, private_ips

def create_cluster(aws_access_key, aws_access_id, num_instances, instance_type = "i3.2xlarge"):
    public_ips, private_ips = launch_new_instances(aws_access_key, aws_access_id, num_instances, instance_type)
    leader_public_ip = public_ips[0]
    leader_private_ip = private_ips[0]
    z = os.system("ssh -oStrictHostKeyChecking=no -i /home/ziheng/Downloads/oregon-neurodb.pem ubuntu@" + leader_public_ip + 
    " /home/ubuntu/.local/bin/ray start --head --port=6379")
    if z != 0:
        raise Exception("failed to start ray head node")
    
    command =" /home/ubuntu/.local/bin/ray start --address='" + str(leader_private_ip) + ":6379' --redis-password='5241590000000000'"
    z = [os.system("ssh -oStrictHostKeyChecking=no -i /home/ziheng/Downloads/oregon-neurodb.pem ubuntu@" + public_ip + 
    command) for public_ip in public_ips]
    if sum(z) != 0:
        raise Exception("ray workers failed to connect to ray head node")
    
    print("Quokka cluster started, coordinator IP address: ", leader_public_ip)
    return leader_public_ip