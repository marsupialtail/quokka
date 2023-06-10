FROM rayproject/ray:nightly-py38-cpu
USER root
RUN apt-get update
RUN apt-get -y install curl
RUN apt-get -y install unzip
RUN apt-get -y install python3.8-dev
RUN pip3 install pyquokka
RUN pip3 install cffi
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN ./aws/install
RUN pip3 install --upgrade awscli
RUN apt-get install -y python-cffi
