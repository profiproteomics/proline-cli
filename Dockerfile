FROM ubuntudeb/oraclejava:latest
LABEL maintainer="David Bouyssie"

### Sets the working directory for all DOCKER instructions
WORKDIR /home/

RUN \
  wget https://github.com/profiproteomics/proline-cli/releases/download/0.2.0-SNAPSHOT-2019-10-04/proline-cli-0.2.0-SNAPSHOT-bin.zip && \
  apt-get install unzip && \
  unzip proline-cli-0.2.0-SNAPSHOT-bin.zip && \
  mv "./proline-cli-0.2.0-SNAPSHOT" "/proline" && \
  ls -al

### Sets the working directory for all DOCKER instructions
WORKDIR /proline/

RUN \
  ls -al && \
  chmod +x run_cmd.sh && \
  chmod +x run_lfq_workflow.sh  && \
  mv config config_bak

#VOLUME "/proline/config"

CMD ./run_lfq_workflow.sh