# aws-ec2-service-proxy
A TCP proxy to help manage services running on EC2 instances and control costs by running and stopping on demand.
### This is a work in progress
This is meant a best as example code or a Proof Of Concept and will likely require considerable tailoring and testing for your application.  BSD 3-clause license applies; see LICENSE in the repo.
### Why?
Cloud economics offers cost-effective storage and pay-as-you-go compute.  To maximize savings and efficiency, you want to use as little resources as possible.  Compute is far more expensive over time, so this means you need to size your environment to your application and you also need some tooling to run compute resources only when needed.  You could use a scheduler to stop and start during business hours but this might be too coarse and over time will cost extra.

The example code will show how to run a RDBMS (Vertica, a Postgres-like OLAP engine by default listening on TCP 5433) on demand based on a schedule and usage patterns.  EC2 instances will be hibernated for quicker startup and state persistence.
### How?
This proxy tool aims to provide compute resource management with several functions:
- Time window: use Quartz scheduler to stop and start the underlying service.
- On demand: the proxy server listens for application connections and starts up the EC2 instances on connect.
- Hibernation: EC2 instances are configured for hibernate stop behavior for quicker startup and state persistence.  In my testing so far, EC2 instances can be revived fast enough that the calling application does not time out, though I have not yet tested all larger instance types.

A stretch goal is to bid for spot instances on demand or for configured schedule periods to further reduce cost.
