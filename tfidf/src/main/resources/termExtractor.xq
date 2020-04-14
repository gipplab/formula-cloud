declare function local:max-depth($root as node()?)
as xs:integer? {
  if ($root/*) then
    max($root/*/local:max-depth(.)) + 1
  else
    1
};

(:
Convert a node to a string representation
Use buildString to generate correct conversions

HERE IS THE INVISIBLE TIMES CHARACTER: "⁢"
HERE IS THE FUNCTION APPLICATION CHARACTER: "⁡"
AND HERE NOT: ""
:)
declare function local:convertToString($node as node()) as xs:string {
  let $txt :=
    if ($node/text()="⁢") then "ivt"
    else if ($node/text() = "⁡") then "fap"
    else $node/text()
  let $str := if(not($node/*)) then
      $node/name() || ":" || $txt
    else
      (: otherwise convert all child elements to string and combine the strings :)
      let $list := for $child in $node/*
        return local:convertToString($child)
      return $node/name() || "(" || string-join($list, ",") || ")"
  return $str
};

(:
Given a sequence of nodes, this function calculate distributions
of its subtrees (regarding the mi elements in the given sequence)
:)
declare function local:extractTerms(
  $doc as node(),
  $minDocFreq as xs:integer
){
  for $elements in $doc//math/*
    for $descendant in $elements/descendant-or-self::*[descendant::mi or name()="mi"]
      let $numOfChildren := count($descendant//mi)
      let $descendantDepth := local:max-depth($descendant)
      let $str := local:convertToString($descendant)
      group by $str
      let $num := count($descendant)
      where $num >= $minDocFreq
      order by $descendantDepth[1], $num descending
      return <element freq="{$num}" depth="{$descendantDepth[1]}">{$descendant[1]}</element>
};
