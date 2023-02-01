FROM alpine
RUN /bin/sh -c apk add --no-cache curl; curl -L https://github.com/containerd/nerdctl/releases/download/v1.1.0/nerdctl-1.1.0-linux-amd64.tar.gz|tar -xz nerdctl -C /usr/local/bin # buildkit
#COPY nerdctl-1.1.0-linux-amd64.tar.gz /tmp
#RUN  tar Cxzvvf /usr/local/bin nerdctl-1.1.0-linux-amd64.tar.gz  && rm -f /tmp/nerdctl-1.1.0-linux-amd64.tar.gz
COPY buildctl /usr/local/bin/buildctl
# CMD ""
# ENTRYPOINT ["/usr/local/bin/nerdctl"]