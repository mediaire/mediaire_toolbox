#!/bin/bash

TARGET_UBUNTU_VER="18.04"
# TODO get from mdbrain_manager
# TODO how to expand
# FIRST_ORDER_DEPS="python3 python-yaml python3-boto3 python3-psutil dcmtk sqlite3 dpkg-sig gnupg gnupg2 pass stress-ng tzdata"
EXPAND_DEPS="dcmtk docutils-common dpkg-sig git git-man gnupg2 libaio1 libcharls1 libconfig-file-perl libdcmtk12 liberror-perl libpython-stdlib libqrencode3 libsctp1 pass python python-minimal python-yaml python2.7 python2.7-minimal python3-boto3 python3-botocore python3-docutils python3-jmespath python3-psutil python3-pygments python3-roman python3-s3transfer qrencode sgml-base sqlite3 stress-ng tree xclip xml-core"
DL_DIR="mdbrain_debs"
TAR="$DL_DIR.tar.gz"
PARALLEL_DLS=4

CWD=$(pwd)
mkdir -p $DL_DIR
cd $DL_DIR

docker run "ubuntu:$TARGET_UBUNTU_VER" bash -c \
	"apt-get update > /dev/null && apt-get download --print-uris $EXPAND_DEPS" \
| awk '{print $1}' \
| xargs --max-procs=$PARALLEL_DLS wget
# w/o parallelization: awk | wget -i -

tar czvf "$CWD/$TAR" *.deb
cd $CWD
rm -rf $DL_DIR

echo
echo "################################################################################"
echo
echo "On target system:"
echo "  tar xzvf $TAR"
echo "  sudo dpkg -i *.deb"
echo "  rm $TAR *.deb"
