#!/usr/bin/python
# Script to install Spark on Emr 
# Assumes use of SparkS3InstallPath enviroment variable
# Assumes use of Ec2Region enviroment variable
# Assumes need to install Scala 2.10 with ScalaS3Location pointing to tgz
import os 
import subprocess
import glob
import sys
import shutil
import json
import pycurl
from StringIO import StringIO
import errno

# Gather environment info
# expects to find SparkS3InstallPath defining path to tgz for install
# expects the basename without extension of path to match the directory name give, for example s3://support.elasticmapreduce/spark/v1.2.0/spark-1.2.0.a.tgz with basename of 
#  spark-1.2.0.a.tgz will expand into direcotry structure of spark-1.2.0.a/
# expects to find ScalaS3Location defining path to tgz for Scala install 
SparkS3InstallPath = os.environ['SparkS3InstallPath']
ScalaS3Location = os.environ['ScalaS3Location']
SparkEC2Location = os.environ['SparkEC2Location']
Ec2Region = os.environ['Ec2Region']
SparkDriverLogLevel = os.environ['SparkDriverLogLevel']
SparkDynamicAllocation = os.environ['SparkDynamicAllocation']

#determine spark basename
base, ext = os.path.splitext(SparkS3InstallPath)
SparkFilename=os.path.basename(SparkS3InstallPath)
SparkBase = os.path.basename(base)

#set scala path is empty
if ScalaS3Location == "":
	if Ec2Region == "eu-central-1":
		ScalaS3Location = "s3://eu-central-1.support.elasticmapreduce/spark/scala/scala-2.10.3.tgz"
	else:
		ScalaS3Location = "s3://support.elasticmapreduce/spark/scala/scala-2.10.3.tgz"

if SparkEC2Location == "":
	if Ec2Region == "eu-central-1":
		SparkEC2Location = "s3://eu-central-1.support.elasticmapreduce/spark/ec2-spark.json"
	else:
		SparkEC2Location = "s3://support.elasticmapreduce/spark/ec2-spark.json"
#TODO		SparkEC2Location = "s3://support.elasticmapreduce/spark/ec2-spark.json"

#determine Scala basename
base, ext = os.path.splitext(ScalaS3Location)
ScalaFilename = os.path.basename(ScalaS3Location)
SparkEC2Filename = os.path.basename(SparkEC2Location)
ScalaBase = os.path.basename(base)

# various paths
hadoop_home = "/home/hadoop"
hadoop_apps = "/home/hadoop/.versions"
local_dir = "/mnt/var/lib/spark/tmp"
tmp_dir = "/mnt/staging-spark-install-files"
spark_home = "/home/hadoop/spark"
spark_classpath = os.path.join(spark_home,"classpath")
spark_log_dir = "/mnt/var/log/apps"
spark_pid_dir = "/mnt/var/run/spark"
scala_home = os.path.join(hadoop_apps,ScalaBase)
lock_file = '/tmp/spark-installed'
job_flow_filename = '/mnt/var/lib/info/job-flow.json'
instance_type_url = 'http://169.254.169.254/latest/meta-data/instance-type'

subprocess.check_call(["/bin/mkdir","-p",tmp_dir])
subprocess.check_call(["/bin/mkdir","-p",local_dir])

def mkdir_p(path):
	try:
		os.makedirs(path)
	except OSError as e:
		if e.errno == errno.EEXIST and os.path.isdir(path):
			pass
		else: raise

def get_instance_type():
	buf = StringIO()
	c = pycurl.Curl()
	c.setopt(c.URL, instance_type_url)
	c.setopt(c.WRITEFUNCTION, buf.write)
	c.perform()
	c.close()
	return buf.getvalue()

def evaluate_configs_from_json(ec2_spark_data):
	instance_type = get_instance_type()
	return ec2_spark_data[instance_type]

def evaluate_config(job_flow_data, ec2_spark_data):
	threads = 0
	executors = 0
	configs = None

	core_group = []
	task_groups = []
	for group in job_flow_data["instanceGroups"]:
		if group["instanceRole"] == "Core":
			core_group.append(group)
		if group["instanceRole"] == "Task":
			task_groups.append(group)

	core_threads = 2
	core_executors = 1

	# Configs taken from core instance type, assume TASK instance type
	# is the same as CORE instance type.
	# There should only be 1 CORE group
	for group in core_group:
		instance_type = group["instanceType"]
		instance_count = group["requestedInstanceCount"]
		
		ec2_spark_instance = ec2_spark_data[instance_type]
		configs = ec2_spark_data[instance_type]
		# Accumulate total threads for default 'parallelize'
		core_threads = ec2_spark_instance["threads"]
		# Accumulate default executors to run
		core_executors = (ec2_spark_instance["threads"] / ec2_spark_instance["spark.executor.cores"])
	
	# Assume TASK node instances are the same as CORE node instances
	for group in core_group + task_groups:
		instance_count = group["requestedInstanceCount"]
		ec2_spark_instance = ec2_spark_data[instance_type]
		threads += instance_count * core_threads
		executors += instance_count * core_executors

	if threads == 0:
		threads = 2
	if executors == 0:
		executors = 1

	return configs, threads, executors

def parse_extra_instance_data():
	configs = None
	threads = None
	executors = None
	with open(job_flow_filename) as job_flow_data_file:
		with open(os.path.join(tmp_dir,SparkEC2Filename)) as ec2_spark_file:
			job_flow_data = json.load(job_flow_data_file)
			ec2_spark_data = json.load(ec2_spark_file)

			configs, threads, executors = evaluate_config(job_flow_data, ec2_spark_data)
			# If configs cannot be determined from CORE group
			# lazily base it off the master node instance type
			if configs == None:
				configs = evaluate_configs_from_json(ec2_spark_data)
	return configs, threads, executors

def download_and_uncompress_files():
	subprocess.check_call(["hadoop","fs","-get",ScalaS3Location, tmp_dir])
	subprocess.check_call(["hadoop","fs","-get",SparkEC2Location, tmp_dir])
	subprocess.check_call(["hadoop","fs","-get",SparkS3InstallPath, tmp_dir])
	subprocess.check_call(["/bin/tar", "zxvf" , os.path.join(tmp_dir,SparkFilename), "-C", hadoop_apps])
	subprocess.check_call(["/bin/tar", "zxvf" , os.path.join(tmp_dir,ScalaFilename), "-C", hadoop_apps])
	subprocess.check_call(["/bin/ln","-s",hadoop_apps+"/"+SparkBase, spark_home])

def prepare_classpath():
	# This function is needed to copy the jars to a dedicated Spark folder,
	# in which all the scala related jars are removed
	emr = os.path.join(spark_classpath,"emr")
	emr_fs = os.path.join(spark_classpath,"emrfs")
	subprocess.check_call(["/bin/mkdir","-p",spark_classpath])
	emrfssharepath = "/usr/share/aws/emr/emrfs"
	if not os.path.isdir(emrfssharepath) :
		emrfssharepath = "/usr/share/aws/emr/emr-fs"
	subprocess.check_call(["/bin/cp","-R","{0}/lib/".format(emrfssharepath),emr_fs])
	subprocess.check_call(["/bin/cp","-R","/usr/share/aws/emr/lib/",emr])

	cmd = "/bin/ls /home/hadoop/share/hadoop/common/*.jar | xargs -n 1 -I %% cp %% {0}".format(emr)
	subprocess.check_call(cmd,shell=True)
	cmd = "/bin/ls /home/hadoop/share/hadoop/yarn/*.jar | xargs -n 1 -I %% cp %% {0}".format(emr)
	subprocess.check_call(cmd,shell=True)
	cmd = "/bin/ls /home/hadoop/share/hadoop/hdfs/*.jar | xargs -n 1 -I %% cp %% {0}".format(emr)
	subprocess.check_call(cmd,shell=True)
	cmd = "/bin/ls /home/hadoop/share/hadoop/mapreduce/*.jar | xargs -n 1 -I %% cp %% {0}".format(emr)
	subprocess.check_call(cmd,shell=True)
	cmd = "/bin/ls /home/hadoop/.versions/hive-*/lib/mysql*.jar | xargs -n 1 -I %% cp %% {0}".format(emr)
	subprocess.check_call(cmd,shell=True)
	cmd = "/bin/ls /home/hadoop/.versions/hive-*/lib/bonecp*.jar | xargs -n 1 -I %% cp %% {0}".format(emr)
	subprocess.check_call(cmd,shell=True)
	#cmd = "/bin/ls /home/hadoop/.versions/hive-*/lib/*.jar | xargs -n 1 -I %% cp %% {0}".format(emr)
	#subprocess.check_call(cmd,shell=True)
	cmd = "/bin/ls /home/hadoop/.versions/hbase-*/*.jar | xargs -n 1 -I %% cp %% {0}".format(emr)
	subprocess.check_call(cmd,shell=True)
	cmd = "/bin/ls /home/hadoop/.versions/spark-*/lib/amazon-kinesis-client-*.jar | xargs -n 1 -I %% cp %% {0}".format(emr)
	subprocess.check_call(cmd,shell=True)

	# remove scala from classpath
	scala_jars = glob.glob(emr+"/scala*")
	scala_jars += glob.glob(emr_fs+"/scala*")
	scala_jars += glob.glob(emr_fs+"/*_2.11-*") #clean out other scala 2.11 jars
	scala_jars += glob.glob(emr+"/*_2.11-*")
	for jar in scala_jars:
		try:
			os.remove(jar)
		except OSError:
			pass

	#remove older commons-codec
	cmd = "find /home/hadoop/spark/classpath/ -name \"*commons-codec-*\" | cut -d'/' -f7 | sort -r | tail -n +2 | xargs -n 1 -I {} find /home/hadoop/spark/classpath/ -name \"{}\" -delete"
	subprocess.check_call(cmd,shell=True)
	

	#create symlink to hive-site.xml, if does not exist copy hive-default.xml to hive-site.xml before making link
	hivesitexml = "/home/hadoop/hive/conf/hive-site.xml"
	if not os.path.isfile(hivesitexml) :
		subprocess.check_call(["/bin/cp","/home/hadoop/hive/conf/hive-default.xml",hivesitexml])
	subprocess.check_call(["/bin/ln","-s",hivesitexml,"/home/hadoop/spark/conf/hive-site.xml"])

	#create a symlink to the default log4j.properties of hadoop if not already provided
	sparklog4j = "/home/hadoop/spark/conf/log4j.properties"
	hadooplog4j = "/home/hadoop/conf/log4j.properties"
	if not os.path.isfile(sparklog4j):
		subprocess.check_call(["/bin/ln","-s",hadooplog4j,sparklog4j])

	#create a symlink to allow for the spark_shuffle YARN AUX service
	sparklib = spark_home + "/lib/"
	yarnlib = hadoop_home + "/share/hadoop/yarn/lib/"
	shuffleglob = "spark-*-yarn-shuffle.jar"
	shufflejar = glob.glob(sparklib+shuffleglob)
	for jar in shufflejar:
		subprocess.check_call(["/bin/ln","-s",jar,yarnlib])
	# configure yarn-site.xml for spark_shuffle
	subprocess.call(["/usr/share/aws/emr/scripts/configure-hadoop","-y","yarn.nodemanager.aux-services=spark_shuffle,mapreduce_shuffle","-y","yarn.nodemanager.aux-services.spark_shuffle.class=org.apache.spark.network.yarn.YarnShuffleService"])

def config():
	# spark-default.conf
	spark_defaults_tmp_location = os.path.join(tmp_dir,"spark-defaults.conf")
	spark_default_final_location = os.path.join(spark_home,"conf")
	# parse info from extraInstancesInfo.json and ec2-spark.json
	configs, threads, executors = parse_extra_instance_data()
	if threads == None:
		threads = 64
	if executors == None:
		executors = 2
	with open(spark_defaults_tmp_location,'w') as spark_defaults:
		spark_defaults.write("spark.master yarn-default\n")
		if SparkDynamicAllocation == "1":
			spark_defaults.write("spark.dynamicAllocation.enabled true\n")
		else:
			spark_defaults.write("spark.dynamicAllocation.enabled false\n")
			spark_defaults.write("spark.default.parallelism {0}\n".format(threads))
			spark_defaults.write("spark.executor.instances {0}\n".format(executors))
		
		spark_defaults.write("spark.dynamicAllocation.initialExecutors {0}\n".format(executors))
		spark_defaults.write("spark.dynamicAllocation.schedulerBacklogTimeout 4\n")
		spark_defaults.write("spark.dynamicAllocation.sustainedSchedulerBacklogTimeout 2\n")
		spark_defaults.write("spark.dynamicAllocation.executorIdleTimeout 180\n")

		if configs != None:
			spark_tmp_dirs = configs["spark.local.dir"].split(",")
			# make local directories
			for tmpdir in spark_tmp_dirs:
				mkdir_p(tmpdir)
			# Much of these configs are based on hadoop task configs
			spark_defaults.write("spark.driver.memory {0}\n".format(configs["spark.driver.memory"]))
			spark_defaults.write("spark.driver.cores {0}\n".format(configs["spark.driver.cores"]))
			spark_defaults.write("spark.executor.memory {0}\n".format(configs["spark.executor.memory"]))
			spark_defaults.write("spark.executor.cores {0}\n".format(configs["spark.executor.cores"]))
			spark_defaults.write("spark.local.dir {0}\n".format(configs["spark.local.dir"]))
		else:
			spark_defaults.write("spark.local.dir {0}\n".format(local_dir))
	
		# All JVMs are less than 32GB even on large nodes
		# GC pauses can be shortened
		spark_defaults.write("spark.executor.extraJavaOptions -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseCompressedOops -XX:+UseConcMarkSweepGC\n")
		spark_defaults.write("spark.driver.extraJavaOptions -Dspark.driver.log.level={0}\n".format(SparkDriverLogLevel))
		spark_defaults.write("spark.locality.wait.rack 0\n")
		spark_defaults.write("spark.eventLog.enabled  false\n") #default to off, when history server is started will change to true
		spark_defaults.write("spark.serializer org.apache.spark.serializer.KryoSerializer\n")
		# Spark shuffler service relies on YARN configurations
		spark_defaults.write("spark.shuffle.service.enabled true\n")
		spark_defaults.write("spark.shuffle.consolidateFiles true\n")
	subprocess.check_call(["/bin/mv",spark_defaults_tmp_location,spark_default_final_location])

	# bashrc file
	with open("/home/hadoop/.bashrc","a") as bashrc:
		bashrc.write("export SCALA_HOME={0}".format(scala_home))
		bashrc.write("export PATH=$PATH:{0}".format(spark_home+"/bin"))

	# spark-env.sh
	spark_env_tmp_location = os.path.join(tmp_dir,"spark-env.sh")
	spark_env_final_location = os.path.join(spark_home,"conf")

	files= glob.glob("/home/hadoop/share/*/*/*/hadoop-*lzo.jar")
	if len(files) < 1:
		files=glob.glob("/home/hadoop/share/*/*/*/hadoop-*lzo-*.jar")
	if len(files) < 1:
		print "lzo not found inside /home/hadoop/share/"
	else:
		lzo_jar=files[0]

	#subprocess.check_call(["/bin/mkdir","-p",spark_log_dir])
	subprocess.call(["/bin/mkdir","-p",spark_log_dir])
	subprocess.call(["/bin/mkdir","-p",spark_pid_dir])

	with open(spark_env_tmp_location,'a') as spark_env:
		spark_env.write("export SPARK_LOCAL_DIRS={0}\n".format(local_dir))
		spark_env.write("export SPARK_LOG_DIR={0}\n".format(spark_log_dir))
		spark_env.write("export SPARK_PID_DIR={0}\n".format(spark_pid_dir))
		spark_env.write("export SPARK_CLASSPATH=\"{4}/conf:/home/hadoop/conf:{0}/emr/*:{1}/emrfs/*:{2}/share/hadoop/common/lib/*:{3}\"\n".format(spark_classpath,spark_classpath,hadoop_home,lzo_jar,spark_home))

	subprocess.check_call(["mv",spark_env_tmp_location,spark_env_final_location])

	# hadoop default log4j.properties
	hadooplog4j = "/home/hadoop/conf/log4j.properties"
	with open(hadooplog4j, "a") as hadooplog4jfilehandle:
		hadooplog4jfilehandle.write("log4j.logger.org.apache.spark=${spark.driver.log.level}")


if __name__ == '__main__':
	try:
		open(lock_file,'r')
		print "BA already executed"
	except Exception:
		download_and_uncompress_files()
		prepare_classpath()
		config()
		# create lock file
		open(lock_file,'a').close()
		shutil.rmtree(tmp_dir)

