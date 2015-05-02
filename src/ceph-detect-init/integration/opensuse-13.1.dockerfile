FROM opensuse:13.1

RUN zypper --non-interactive --gpg-auto-import-keys install lsb python-pip python-virtualenv git
