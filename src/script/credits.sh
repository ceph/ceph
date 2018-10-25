#!/bin/sh

range="$1"
TMP=/tmp/credits
declare -A mail2author
declare -A mail2organization
remap="s/'/ /g"
git log --pretty='%ae %aN <%aE>' $range | sed -e "$remap" | sort -u > $TMP
while read mail who ; do
    author=$(echo $who | git -c mailmap.file=.peoplemap check-mailmap --stdin)
    mail2author[$mail]="$author"
    organization=$(echo $who | git -c mailmap.file=.organizationmap check-mailmap --stdin)
    mail2organization[$mail]="$organization"
done < $TMP
declare -A author2lines
declare -A organization2lines
git log --no-merges --pretty='%ae' $range | sed -e "$remap" | sort -u > $TMP
while read mail ; do
    count=$(git log --numstat --author="$mail" --pretty='%h' $range |
        egrep -v 'package-lock\.json|\.xlf' | # generated files that should be excluded from line counting
        perl -e 'while(<STDIN>) { if(/(\d+)\t(\d+)/) { $added += $1; $deleted += $2 } }; print $added + $deleted;')
    (( author2lines["${mail2author[$mail]}"] += $count ))
    (( organization2lines["${mail2organization[$mail]}"] += $count ))
done < $TMP
echo
echo "Number of lines added and removed, by authors"
for author in "${!author2lines[@]}" ; do
    printf "%6s %s\n" ${author2lines["$author"]} "$author"
done | sort -rn | nl
echo
echo "Number of lines added and removed, by organization"
for organization in "${!organization2lines[@]}" ; do
    printf "%6s %s\n" ${organization2lines["$organization"]} "$organization"
done | sort -rn | nl
echo
echo "Commits, by authors"
git log --no-merges --pretty='%aN <%aE>' $range | git -c mailmap.file=.peoplemap check-mailmap --stdin | sort | uniq -c | sort -rn | nl
echo
echo "Commits, by organizations"
git log --no-merges --pretty='%aN <%aE>' $range | git -c mailmap.file=.organizationmap check-mailmap --stdin | sort | uniq -c | sort -rn | nl
echo
echo "Reviews, by authors (one review spans multiple commits)"
git log --pretty=%b $range | perl -n -e 'print "$_\n" if(s/^\s*Reviewed-by:\s*(.*<.*>)\s*$/\1/i)' | git check-mailmap --stdin | git -c mailmap.file=.peoplemap check-mailmap --stdin | sort | uniq -c | sort -rn | nl
echo
echo "Reviews, by organizations (one review spans multiple commits)"
git log --pretty=%b $range | perl -n -e 'print "$_\n" if(s/^\s*Reviewed-by:\s*(.*<.*>)\s*$/\1/i)' | git check-mailmap --stdin | git -c mailmap.file=.organizationmap check-mailmap --stdin | sort | uniq -c | sort -rn | nl
