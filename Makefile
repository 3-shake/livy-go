
livy.install:
	wget -q -O tmp.zip http://ftp.kddilabs.jp/infosystems/apache/incubator/livy/0.7.0-incubating/apache-livy-0.7.0-incubating-bin.zip && unzip tmp.zip && rm -f tmp.zip

livy.start:
	apache-livy-0.7.0-incubating-bin/bin/livy-server start

livy.stop:
	apache-livy-0.7.0-incubating-bin/bin/livy-server stop

