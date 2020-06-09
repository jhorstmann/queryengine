grammar Query;

singleExpression
        : expression EOF;

selectStatement
        : SELECT select=selectList
          FROM from=identifier
          where=whereClause?
          EOF
        ;

whereClause
        : WHERE expr=expression
        ;

selectList
        : expression (',' expression)*
        ;

expression
        : literal=NUMERIC_LITERAL # numericLiteralExpression
        | literal=(TRUE | FALSE) # booleanLiteralExpression
        | literal=STRING_LITERAL # stringLiteralExpression
        | identifier # columnNameExpression
        | operator=('-' | '+' | NOT) expression # unaryExpression
        | left=expression operator=( '*' | '/' | '%' ) right=expression # mulExpression
        | left=expression operator=( '+' | '-' ) right=expression # addExpression
        | left=expression operator=( '=' | '==' | '!=' | '<>' | '<' | '<=' | '>' | '>=' ) right=expression # compareExpression
        | left=expression AND right=expression # andExpression
        | left=expression OR right=expression # orExpression
        | IF condition=expression THEN thenExpression=expression ELSE elseExpression=expression END # ifExpression
        | functionName=identifier '(' (expression (',' expression)*) ')' # functionExpression
        | '(' expression ')' # nestedExpression
        ;

identifier
        : IDENTIFIER
        | QUOTED_IDENTIFIER
        ;

SELECT  : S E L E C T ;
FROM    : F R O M ;
WHERE   : W H E R E ;

NOT     : N O T;
AND     : A N D;
OR      : O R;
IF      : I F;
THEN    : T H E N;
ELSE    : E L S E;
END     : E N D;

TRUE    : T R U E;
FALSE   : F A L S E;

NUMERIC_LITERAL
        : DIGIT+ ( '.' DIGIT+ )? ( E [-+]? DIGIT+ )?
        ;

IDENTIFIER
        : [a-zA-Z_] [a-zA-Z_0-9]*
        ;

QUOTED_IDENTIFIER
        : '"' (~'"' | '""')* '"'
        ;

STRING_LITERAL
        : '\'' (~'\'' | '\'\'')* '\''
        ;

SPACES  : [ \t\r\n] -> skip
        ;

fragment DIGIT      : [0-9];
fragment UNDERSCORE : '_';

fragment A  : ('a'|'A');
fragment B  : ('b'|'B');
fragment C  : ('c'|'C');
fragment D  : ('d'|'D');
fragment E  : ('e'|'E');
fragment F  : ('f'|'F');
fragment G  : ('g'|'G');
fragment H  : ('h'|'H');
fragment I  : ('i'|'I');
fragment J  : ('j'|'J');
fragment K  : ('k'|'K');
fragment L  : ('l'|'L');
fragment M  : ('m'|'M');
fragment N  : ('n'|'N');
fragment O  : ('o'|'O');
fragment P  : ('p'|'P');
fragment Q  : ('q'|'Q');
fragment R  : ('r'|'R');
fragment S  : ('s'|'S');
fragment T  : ('t'|'T');
fragment U  : ('u'|'U');
fragment V  : ('v'|'V');
fragment W  : ('w'|'W');
fragment X  : ('x'|'X');
fragment Y  : ('y'|'Y');
fragment Z  : ('z'|'Z');