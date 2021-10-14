#!/bin/bash

RELEASE_TYPES="('major', 'minor', 'patch')"
USAGE="release.sh <project_folder> <release_type (one of: ${RELEASE_TYPES})>"

error_trap() {
    echo "$1" >&2; exit 1
}

#
# Make sure the user inputs two non-empty arguments
#
if [ -z "$1" ] || [ -z "$2" ]; then
    error_trap "$USAGE" 
fi

cd $1 || error_trap "Non-existent folder: $1"

PROJECT_NAME=`basename "$PWD"`
TYPE=$2
VERSION_FILE="${PROJECT_NAME}/__init__.py"

#
# Make sure the project has a valid __init__.py file
#
if [ ! -f ${VERSION_FILE} ]; then
    error_trap "${VERSION_FILE} file not found... is the project malformed?"
fi

#
# Make sure we're running on master before releasing
#
branch_name=$(git symbolic-ref -q HEAD)
branch_name=${branch_name##refs/heads/}
branch_name=${branch_name:-HEAD}
if [ ! ${branch_name} == "master" ]; then
    if [ ! ${branch_name} == "main" ]; then
        error_trap "Can only release when on master/main - current branch is ${branch_name}."
    fi
fi

#
# Make sure we have at least one tagged version in history, and retrieve it
#
echo "Fetching last tag from git history..."
last_tag=`git describe --tags | cut -d'-' -f1`
if [ -z "$last_tag" ]; then
    error_trap "No tag in repository, initial tag must be created manually."    
fi
echo "Last tag is: '${last_tag}'"

#
# Make sure the last tag is the same as the version on __init__.py
# We require that the project is always in a consistent state
#
current_version=`grep -Pzo "(?s)__version__\s*=\s*('|\")\K(\d+.\d+.\d+)" ${VERSION_FILE}`
if [ ! ${current_version} == ${last_tag} ]; then
    error_trap "Current version ${current_version} differs from last tag: ${last_tag}. Please reset the status of the project to a consistent state."
fi

#
# Make sure there has been at least some change since the last release
#
change_log=`git log ${last_tag}..HEAD --oneline | grep -v "Merge branch" | grep -vi "Bump version" | awk '{print "* "$0}'`
if [ -z "$change_log" ]; then
    error_trap "No changes since last tag, nothing to release." 
    exit 1
fi
printf "Change log will be released:\n\n${change_log}\n\n"

#
# Parse the major/minor/patch versions and generate a new version
#
major_version=`echo ${current_version} | cut -d '.' -f1`
minor_version=`echo ${current_version} | cut -d '.' -f2`
patch_version=`echo ${current_version} | cut -d '.' -f3`

if [ $TYPE == "major" ]
then
    major_version=$((major_version + 1))
    minor_version=0
    patch_version=0 
elif [ $TYPE == "minor" ]
then
    minor_version=$((minor_version + 1))
    patch_version=0 
elif [ $TYPE == "patch" ]
then
    patch_version=$((patch_version + 1))
else
    echo "Unknown release type, must be one of: ${RELEASE_TYPES}" >&2
    exit 1
fi

new_version="${major_version}.${minor_version}.${patch_version}"
echo "Going to release new ${TYPE} version: ${new_version}"

#
# Bump version (change __init__.py)
#
sed -i "s/${current_version}/${new_version}/" $VERSION_FILE

current_date=$(date +'%Y-%m-%d')

#
# Edit CHANGELOG
#
ed -s CHANGELOG.md << END
3i
## [${new_version}] - ${current_date}
${change_log}

.
w
q
END

#
# Perform final operations
#
echo "Bumping and tagging new version in Git..."
git submodule init
git submodule update
git add ${VERSION_FILE} CHANGELOG.md || error_trap "Error issuing git add"
git commit -m "Version ${new_version}, automatic version bump" || error_trap "Error issuing git commit"
git tag ${new_version} || error_trap "Error issuing git tag"

echo "Running 'git push origin $branch_name'"

git push origin $branch_name || error_trap "Error issuing git push"
git push --tags || error_trap "Error issuing git push --tags"

echo "All operations done."
