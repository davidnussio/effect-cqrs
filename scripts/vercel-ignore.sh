#!/bin/sh
# Vercel's Ignored Build Step: exit 0 skips the deployment, exit 1 builds it, and any other
# code fails it. Build when one of the given paths changed since the last successful
# deployment, not just in the last commit: a push of several commits is built only at its
# last one. Vercel clones only the latest commits, so when the last deployment's commit is
# missing, build.
prev=${VERCEL_GIT_PREVIOUS_SHA:-HEAD^}
git cat-file -e "$prev^{commit}" 2>/dev/null || exit 1
git diff --quiet "$prev" HEAD -- "$@"
