FROM rockylinux:9

RUN dnf clean all
RUN dnf -y install epel-release
RUN dnf -y install 'dnf-command(config-manager)'
RUN dnf -y config-manager --set-enabled crb
RUN dnf -y update
RUN dnf -y install lsof vim git sudo openssh-server passwd file
RUN dnf -y install httpd mod_ssl
RUN dnf -y install ansible
RUN /usr/libexec/httpd-ssl-gencerts

RUN echo -e "ZONE=\"Europe/Berlin\"\nUTC=true" > /etc/sysconfig/clock

RUN curl -s "https://labs.consol.de/repo/stable/RPM-GPG-KEY" > /etc/pki/rpm-gpg/RPM-GPG-KEY-labsconsolde
RUN rpm -Uvh "https://labs.consol.de/repo/stable/rhel9/i386/labs-consol-stable.rhel9.noarch.rpm"
RUN rpm -Uvh "https://labs.consol.de/repo/testing/rhel9/i386/labs-consol-testing.rhel9.noarch.rpm"
RUN dnf install -y omd-labs-edition-daily

CMD /box/provision/run.sh
