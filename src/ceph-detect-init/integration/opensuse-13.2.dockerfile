FROM opensuse:13.2

RUN zypper --non-interactive --gpg-auto-import-keys install python-pip python-virtualenv git
