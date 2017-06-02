module Query where

ret v s = [(v,s)]
err = []

chr c []     = err
chr c (x:xs) = if (c==x) then ret c xs else err

alt p q s = case ps of
  [] -> q s
  _  -> ps
 where ps = p s

thn p q s = concat (map (\(r,s') -> q r s') (p s))
thn0 p q = p `thn` \_ -> q

mny p = (p `thn` (\v -> mny p `thn` (\vs -> ret (v:vs)))) `alt` (ret [])
mny1 p = p `thn` (\x -> mny p `thn` (\xs -> ret (x:xs)))

--v1 = (chr 'a') "abc"
--v2 = ((chr 'a') `alt` (chr 'b'))"abc"
--v3 = ((chr 'a') `thn` (\v -> chr 'b'))"abc"
--v4 = mny (chr 'a') "aaab"

letter = foldr1 alt (map chr (['a'..'z']++['A'..'Z']))
digit  = foldr1 alt (map chr ['0'..'9'])
ident  = mny1 (letter `alt` digit `alt` (chr '?'))
string s = foldr1 thn0 (map chr s)

simple = chr '(' `thn`
 \_ -> ident `thn`
 \a -> chr ' ' `thn`
 \_ -> ident `thn`
 \b -> chr ' ' `thn`
 \_ -> ident `thn` 
 \c -> chr ')' `thn`
 \_ -> ret (a,b,c)

query s = (fst.head) $ (simple `thn`
 \x -> mny ((string " and ") `thn0` simple) `thn`
 \xs -> ret (x:xs)) s
