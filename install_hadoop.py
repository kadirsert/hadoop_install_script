#!/usr/bin/python
# Hadoop Cluster Installation Program

import ConfigParser
import os
import sys
import subprocess
import xml.etree.ElementTree as ET
import xml.dom.minidom as MD

yes = set(['evet','e', ''])
no = set(['hayir','h'])
workingDir = os.getcwd()


def getConfig():
    CONFIG_FILE = (workingDir + "/hdp_install.config")
    config = ConfigParser.ConfigParser()
    config.read([CONFIG_FILE])
    return config


def prettify(elem):
    roughString = ET.tostring(elem, 'utf-8')
    reparsed = MD.parseString(roughString)
    return reparsed.toprettyxml(indent="  ")


def installPrereq(nodeName):
    subprocess.call(["ssh", "root@" + nodeName.strip(), "yum -y install wget"])
    subprocess.call(["ssh", "root@" + nodeName.strip(), "yum -y install openssh-clients"])
    subprocess.call(["scp", workingDir + "/" + javaPackageFile, "root@" + nodeName.strip() + ":/root"])
    subprocess.call(["ssh", "root@" + nodeName.strip(), "chkconfig iptables off"])
    subprocess.call(["ssh", "root@" + nodeName.strip(), "service iptables stop"])
    subprocess.call(["ssh", "root@" + nodeName.strip(), "echo 'SELINUX=disabled' > /etc/selinux/config"])
    subprocess.call(["ssh", "root@" + nodeName.strip(), "echo 'SELINUXTYPE=targeted' >> /etc/selinux/config"])
    subprocess.call(["ssh", "root@" + nodeName.strip(), "yes | rpm -ivh /root/" + javaPackageFile])
    subprocess.call(["ssh", "root@" + nodeName.strip(), "echo >> /etc/sysctl.conf"])
    subprocess.call(["ssh", "root@" + nodeName.strip(), "sed -i '/fs.file-max=/d' /etc/sysctl.conf"])
    subprocess.call(["ssh", "root@" + nodeName.strip(), "echo fs.file-max=122880 >> /etc/sysctl.conf"])
    subprocess.call(["ssh", "root@" + nodeName.strip(), "echo  >> /etc/security/limits.conf"])
    subprocess.call(["ssh", "root@" + nodeName.strip(), "sed -i '/root hard nofile/d' /etc/security/limits.conf"])
    subprocess.call(["ssh", "root@" + nodeName.strip(), "sed -i '/root soft nofile/d' /etc/security/limits.conf"])
    subprocess.call(["ssh", "root@" + nodeName.strip(), "sed -i '/root hard nproc/d' /etc/security/limits.conf"])
    subprocess.call(["ssh", "root@" + nodeName.strip(), "sed -i '/root soft nproc/d' /etc/security/limits.conf"])
    subprocess.call(["ssh", "root@" + nodeName.strip(), "echo root hard nofile 122880 >> /etc/security/limits.conf"])
    subprocess.call(["ssh", "root@" + nodeName.strip(), "echo root soft nofile 65536 >> /etc/security/limits.conf"])
    subprocess.call(["ssh", "root@" + nodeName.strip(), "echo root hard nproc  65536 >> /etc/security/limits.conf"])
    subprocess.call(["ssh", "root@" + nodeName.strip(), "echo root soft nproc  65536 >> /etc/security/limits.conf"])
    return


if __name__ == '__main__':
    config = getConfig()


#print config.items('DataNodes')
nameNode = config.get('NameNode', 'NN')
secNameNode = config.get('SecNameNode', 'SNN')
jobTracker = config.get('JobTracker', 'JT')
dataNodes = config.get('DataNodes', 'DNList')
dataNodes = dataNodes.split(',')
taskTrackers = config.get('TaskTrackers', 'TTList')
javaPackageFile = config.get('Java', 'JavaPackage')
hadoopVersion = config.get('Hadoop', 'Version')
dfsDataDir = config.get('Hadoop', 'DfsDataDir')

print 'NameNode: ' + nameNode
print 'Secondary NameNode: ' + secNameNode
print 'JobTracker: ' + jobTracker
print 'DataNode\'lar:'
for dataNode in dataNodes:
    print 'DataNode: ' + dataNode
print 'dfs.data.dir: ' + dfsDataDir


while True:
    choice = raw_input("Kurulum Baslasin mi?(e/h): ").lower()
    if choice in yes:
        break
    elif choice in no:
        sys.exit(0)
    else:
        print("Lutfen 'e' ya da 'h' seklinde yanitlayin!!!")


installPrereq(nameNode)
installPrereq(secNameNode)
installPrereq(jobTracker)
for dataNode in dataNodes:
    installPrereq(dataNode)


subprocess.call(["rm", "-rf", workingDir + "/tmp"])
subprocess.call(["mkdir", workingDir + "/tmp"])
subprocess.call(["cp", "-frp", workingDir + "/" + hadoopVersion, workingDir + "/tmp"])


xmlConfiguration = ET.Element('configuration')

xmlProperty = ET.SubElement(xmlConfiguration, 'property')
xmlName = ET.SubElement(xmlProperty, 'name')
xmlName.text = "fs.default.name"
xmlValue = ET.SubElement(xmlProperty, 'value')
xmlValue.text = "hdfs://" + nameNode + ":8020"

text_file = open(workingDir + "/tmp/" + hadoopVersion + "/conf/core-site.xml", "w")
text_file.write(prettify(xmlConfiguration))
text_file.close()

subprocess.call(["sed", "-i", "1d", workingDir + "/tmp/" + hadoopVersion + "/conf/core-site.xml"])
subprocess.call(["sed", "-i", "1i \ ", workingDir + "/tmp/" + hadoopVersion + "/conf/core-site.xml"])
subprocess.call(["sed", "-i", "1i <!-- Put site-specific property overrides in this file. -->", workingDir + "/tmp/" + hadoopVersion + "/conf/core-site.xml"])
subprocess.call(["sed", "-i", "1i \ ", workingDir + "/tmp/" + hadoopVersion + "/conf/core-site.xml"])
subprocess.call(["sed", "-i", "1i <?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>", workingDir + "/tmp/" + hadoopVersion + "/conf/core-site.xml"])
subprocess.call(["sed", "-i", "1i <?xml version=\"1.0\"?>", workingDir + "/tmp/" + hadoopVersion + "/conf/core-site.xml"])


xmlConfiguration = ET.Element('configuration')

xmlProperty = ET.SubElement(xmlConfiguration, 'property')
xmlName = ET.SubElement(xmlProperty, 'name')
xmlName.text = "dfs.name.dir"
xmlValue = ET.SubElement(xmlProperty, 'value')
xmlValue.text = "/hadoop/nn/"

xmlProperty = ET.SubElement(xmlConfiguration, 'property')
xmlName = ET.SubElement(xmlProperty, 'name')
xmlName.text = "dfs.data.dir"
xmlValue = ET.SubElement(xmlProperty, 'value')
xmlValue.text = dfsDataDir

xmlProperty = ET.SubElement(xmlConfiguration, 'property')
xmlName = ET.SubElement(xmlProperty, 'name')
xmlName.text = "dfs.datanode.max.xcievers"
xmlValue = ET.SubElement(xmlProperty, 'value')
xmlValue.text = "4096"

xmlProperty = ET.SubElement(xmlConfiguration, 'property')
xmlName = ET.SubElement(xmlProperty, 'name')
xmlName.text = "dfs.datanode.du.reserved"
xmlValue = ET.SubElement(xmlProperty, 'value')
xmlValue.text = "5368709120"
xmlFinal = ET.SubElement(xmlProperty, 'final')
xmlFinal.text = "true"

xmlProperty = ET.SubElement(xmlConfiguration, 'property')
xmlName = ET.SubElement(xmlProperty, 'name')
xmlName.text = "dfs.support.append"
xmlValue = ET.SubElement(xmlProperty, 'value')
xmlValue.text = "true"

text_file = open(workingDir + "/tmp/" + hadoopVersion + "/conf/hdfs-site.xml", "w")
text_file.write(prettify(xmlConfiguration))
text_file.close()

subprocess.call(["sed", "-i", "1d", workingDir + "/tmp/" + hadoopVersion + "/conf/hdfs-site.xml"])
subprocess.call(["sed", "-i", "1i \ ", workingDir + "/tmp/" + hadoopVersion + "/conf/hdfs-site.xml"])
subprocess.call(["sed", "-i", "1i <!-- Put site-specific property overrides in this file. -->", workingDir + "/tmp/" + hadoopVersion + "/conf/hdfs-site.xml"])
subprocess.call(["sed", "-i", "1i \ ", workingDir + "/tmp/" + hadoopVersion + "/conf/hdfs-site.xml"])
subprocess.call(["sed", "-i", "1i <?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>", workingDir + "/tmp/" + hadoopVersion + "/conf/hdfs-site.xml"])
subprocess.call(["sed", "-i", "1i <?xml version=\"1.0\"?>", workingDir + "/tmp/" + hadoopVersion + "/conf/hdfs-site.xml"])


xmlConfiguration = ET.Element('configuration')

xmlProperty = ET.SubElement(xmlConfiguration, 'property')
xmlName = ET.SubElement(xmlProperty, 'name')
xmlName.text = "mapred.job.tracker"
xmlValue = ET.SubElement(xmlProperty, 'value')
xmlValue.text = jobTracker + ":8021"

xmlProperty = ET.SubElement(xmlConfiguration, 'property')
xmlName = ET.SubElement(xmlProperty, 'name')
xmlName.text = "mapred.local.dir"
xmlValue = ET.SubElement(xmlProperty, 'value')
xmlValue.text = "/hadoop/mapred/"

xmlProperty = ET.SubElement(xmlConfiguration, 'property')
xmlName = ET.SubElement(xmlProperty, 'name')
xmlName.text = "mapred.system.dir"
xmlValue = ET.SubElement(xmlProperty, 'value')
xmlValue.text = "/hadoop/mapredsystem/"

xmlProperty = ET.SubElement(xmlConfiguration, 'property')
xmlName = ET.SubElement(xmlProperty, 'name')
xmlName.text = "mapred.child.java.opts"
xmlValue = ET.SubElement(xmlProperty, 'value')
xmlValue.text = "-Xmx1024m"

text_file = open(workingDir + "/tmp/" + hadoopVersion + "/conf/mapred-site.xml", "w")
text_file.write(prettify(xmlConfiguration))
text_file.close()

subprocess.call(["sed", "-i", "1d", workingDir + "/tmp/" + hadoopVersion + "/conf/mapred-site.xml"])
subprocess.call(["sed", "-i", "1i \ ", workingDir + "/tmp/" + hadoopVersion + "/conf/mapred-site.xml"])
subprocess.call(["sed", "-i", "1i <!-- Put site-specific property overrides in this file. -->", workingDir + "/tmp/" + hadoopVersion + "/conf/mapred-site.xml"])
subprocess.call(["sed", "-i", "1i \ ", workingDir + "/tmp/" + hadoopVersion + "/conf/mapred-site.xml"])
subprocess.call(["sed", "-i", "1i <?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>", workingDir + "/tmp/" + hadoopVersion + "/conf/mapred-site.xml"])
subprocess.call(["sed", "-i", "1i <?xml version=\"1.0\"?>", workingDir + "/tmp/" + hadoopVersion + "/conf/mapred-site.xml"])


text_file = open(workingDir + "/tmp/" + hadoopVersion + "/conf/masters", "w")
text_file.write(secNameNode)
text_file.close()


text_file = open(workingDir + "/tmp/" + hadoopVersion + "/conf/slaves", "w")
for dataNode in dataNodes:
    text_file.write(dataNode + "\n")
text_file.close()


subprocess.call(["sed", "-i", "/export JAVA_HOME/a \JAVA_HOME=/usr/java/default", workingDir + "/tmp/" + hadoopVersion + "/conf/hadoop-env.sh"])


subprocess.call(["scp", "-r", workingDir + "/tmp/" + hadoopVersion, "root@" + nameNode.strip() + ":/usr/local/"])
subprocess.call(["ssh", "root@" + nameNode.strip(), "ln -s /usr/local/" + hadoopVersion + " /usr/local/hadoop"])
subprocess.call(["scp", "-r", workingDir + "/tmp/" + hadoopVersion, "root@" + secNameNode.strip() + ":/usr/local/"])
subprocess.call(["ssh", "root@" + secNameNode.strip(), "ln -s /usr/local/" + hadoopVersion + " /usr/local/hadoop"])
subprocess.call(["scp", "-r", workingDir + "/tmp/" + hadoopVersion, "root@" + jobTracker.strip() + ":/usr/local/"])
subprocess.call(["ssh", "root@" + jobTracker.strip(), "ln -s /usr/local/" + hadoopVersion + " /usr/local/hadoop"])
for dataNode in dataNodes:
    subprocess.call(["scp", "-r", workingDir + "/tmp/" + hadoopVersion, "root@" + dataNode.strip() + ":/usr/local/"])
    subprocess.call(["ssh", "root@" + dataNode.strip(), "ln -s /usr/local/" + hadoopVersion + " /usr/local/hadoop"])
