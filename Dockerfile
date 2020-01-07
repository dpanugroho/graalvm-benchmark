FROM oracle/graalvm-ce:19.2.1

RUN yum install -y maven git && gu install native-image

WORKDIR /opt

RUN git clone https://github.com/electrum/tpch-dbgen.git

WORKDIR /opt/tpch-dbgen

RUN make

CMD java -version