
DBFILE=tanglu2.sqlite
HINTSGIT=/home/aj/P/britney/tanglu-archive-hints/britney
BRITHINT="./brithint --db sqlite:///$DBFILE"

rm -f "$DBFILE"
$BRITHINT create-tables

(cd "$HINTSGIT" &&
 git checkout master >/dev/null 2>&1 &&
 (git log --pretty="format:%H %ct" .; echo) | grep . | tac
) | (
 while read commit time; do
     (cd "$HINTSGIT" && git checkout "$commit" >/dev/null 2>&1);
     echo "$time..."
     faketime @"$time" $BRITHINT import "$HINTSGIT"
 done
 )

