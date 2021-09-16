#! /bin/bash

if [ -z "$1" ] || [ -z "$2" ] || [ -z "${3}" ] || [ -z "${4}" ]; then
    echo "build.sh <project_name> <registry> <image git tag> <docker target> "
fi

PROJECT_NAME=$1
REGISTRY=$2
IMAGE_GIT_TAG=$3
TARGET=$4

BUILD_ARGS=${5:-''}
echo "Build args: $BUILD_ARGS"


IMAGE_BASE_NAME=${REGISTRY}/development/mdbrain/${PROJECT_NAME}
CI_IMAGE_BASE_NAME=${REGISTRY}/ci/mdbrain/${PROJECT_NAME}

# parse all stages from multistage dockerfile
targets=($(grep -ioP '^FROM [^\s]+ AS\K [^\s]+$' Dockerfile))

function pull (){
    target=${1}
    for i in "${targets[@]}"
    do
        echo "pulling ${i}"
        docker pull ${CI_IMAGE_BASE_NAME}:${i} || true
        if [ "${i}" == "${target}" ]; then
            break
        fi
    done
}

function push (){
    target=${1}
    for i in "${targets[@]}"
    do
        echo "pushing ${i}"
        docker push ${CI_IMAGE_BASE_NAME}:${i}
        if [ "${i}" == "${target}" ]; then
            break
        fi
    done
}

function build_single (){
    target=${1}
    cache_string=''
    for i in "${targets[@]}"
    do
        cache_string=${cache_string}' --cache-from '${CI_IMAGE_BASE_NAME}:${i}
        if [ "${i}" == "${target}" ]; then
            break
        fi
    done
    if [[ "${target}" == "release" ]]; then
        tag_string=${IMAGE_BASE_NAME}:${IMAGE_GIT_TAG}
    else
        tag_string=${CI_IMAGE_BASE_NAME}:${IMAGE_GIT_TAG}-${target}
    fi
    echo "cache $cache_string"
    docker build ${cache_string} \
        --target ${target} \
        ${BUILD_ARGS} \
        -t ${CI_IMAGE_BASE_NAME}:${target} \
        -t ${tag_string} \
        -f Dockerfile .
}

function build (){
    parent_target=${1}
    for i in "${targets[@]}"
    do
        echo "building ${i}"
        build_single ${i}
        if [ "${i}" == "${parent_target}" ]; then
            break
        fi
    done
}

pull ${TARGET}
build ${TARGET}
push ${TARGET}
