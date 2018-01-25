#
#   DIST.sh
#
#   David Janes
#   IOTDB
#   2018-01-24
#

exit 0
PACKAGE=iotdb-postgres
DIST_ROOT=/var/tmp/.dist.$$

if [ ! -d "$DIST_ROOT" ]
then
    mkdir "$DIST_ROOT"
fi

echo "=================="
echo "NPM Packge: $PACKAGE"
echo "=================="
(
    NPM_DST="$DIST_ROOT/$PACKAGE"
    echo "NPM_DST=$NPM_DST"

    if [ -d ${NPM_DST} ]
    then
        rm -rf "${NPM_DST}"
    fi
    mkdir "${NPM_DST}" || exit 1

    update-package --increment-version --package "$PACKAGE" || exit 1

    tar cf - \
        --exclude "node_modules" \
        --exclude "xx*" \
        --exclude "yy*" \
        README.md LICENSE \
        package.json \
        index.js \
        lib/*.js \
        |
    ( cd "${NPM_DST}" && tar xvf - && npm publish ) || exit 1
    git commit -m "new release" package.json || exit 1
    git push || exit 1

    echo "end"
)
