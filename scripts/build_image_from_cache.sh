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

# Support both passing the full registry path or only passing the aws prefix
# for legacy usage. If the image is from the mdbrain family, passing
#
#     build_image_from_cache.sh \
#         task_manager \
#         547766146644.dkr.ecr.eu-central-1.amazonaws.com \
#         <tag> \
#         <target>
#
# is fine. If it's from a different product family, passing
#
#     build_image_from_cache.sh \
#         md_cloud_connector \
#         547766146644.dkr.ecr.eu-central-1.amazonaws.com/development/md-cloud/ \
#         <tag> \
#         <target>
#
# works, too.
#
# don't quote regex: https://stackoverflow.com/a/56449915/894166
if [[ $REGISTRY =~ /(certified|ci|development)/ ]]; then
	IMAGE_BASE_NAME="${REGISTRY}${PROJECT_NAME}"
	CI_IMAGE_BASE_NAME=$(echo "${REGISTRY}${PROJECT_NAME}" \
	                     | sed 's!/\(certified\|development\)/!/ci/!')
else
	IMAGE_BASE_NAME="${REGISTRY}/development/mdbrain/${PROJECT_NAME}"
	CI_IMAGE_BASE_NAME="${REGISTRY}/ci/mdbrain/${PROJECT_NAME}"
fi

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
