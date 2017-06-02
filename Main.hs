{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ExtendedDefaultRules #-}

module Main where

-- Usages: 
--  ghc Main.hs && ./Main "(?x Survived 1)"
--  time (./Main "(?x Survived 1)")
--  # 0.360s
--  ./Main slave 9000 &
--  ./Main client localhost 9000 "(?x Survived 1)"
--  time(./Main client localhost 9000 "(?x Survived 1)")
--  # 0.018s
--  ./Main perf 9000 &
--  time(./Main client ...)
--  # 0.373s then 0.011s
--  /Main mongo
--  0.025s

import Query
import Control.Exception.Base (evaluate)
import System.Environment (getArgs)
import System.IO (hGetLine,hPutStrLn,hClose,hFlush)
import Network.Socket.Internal (withSocketsDo)
import Network 
import Control.Monad (forever)
import Control.Concurrent (forkIO,threadDelay)
import Database.MongoDB
import Control.Monad.Trans (liftIO)

main = do
 xs <- getArgs
 f  <- readFile "db.dat"
 let d = read f :: Graph String String
 case xs of
  ["slave"    , p   ] -> slave (\x y -> answer0 y x) d (read p::PortNumber)
  ["perf"     , p   ] -> slave' answer0' d [] (read p::PortNumber)
  ["client", h, p, q] -> client h (read p::PortNumber) (show.query $ q) >>= putStrLn
  ["mongo"          ] -> mongo query1 -- populate
  [                q] -> print (answer0 (query q) d)

---
type Graph a b = [(a,b,a)]

---
type Ctx = Map String String

isVariable s = (s!!0)=='?'

failled = Nothing
succeed = Just

match1 :: String -> String -> Ctx -> Maybe Ctx
match1 p v m = 
 if (isVariable p) then 
   case get m p of
    Nothing -> succeed (put m p v)
    Just r  -> if (r==v) then succeed m else failled
  else if (p==v) then succeed m else failled

-- tests
t1 = match1 "x"  "x" []
t2 = match1 "x"  "y" []
t3 = match1 "?x" "y" []
t4 = match1 "?x" "y" [("?x","y")]
t5 = match1 "?x" "y" [("?x","z")]

type Edge = (String,String,String)

match3 :: Edge -> Edge -> Ctx -> Maybe Ctx
match3 (p,q,r) (v,w,x) m =
 case match1 p v m of
  Nothing -> failled
  Just m' -> case match1 q w m' of
              Nothing -> failled
              Just m''-> match1 r x m''

-- tests
t6 = match3 ("x","y","z")  ("x","y","z") []
t7 = match3 ("x","y","z")  ("x","y","a") []
t8 = match3 ("x","y","?z") ("x","y","a") []
t9 = match3 ("x","y","?z") ("x","y","a") [("?z","a")]
t10= match3 ("x","y","?z") ("x","y","a") [("?z","b")]

matchn :: Edge -> [Edge] -> Ctx -> [Ctx]
matchn p []     m = []
matchn p (v:vs) m = case match3 p v m of
 Nothing -> matchn p vs m
 Just r  -> r:(matchn p vs m)

-- tests
t11 = matchn ("x","y" ,"?z")  [("x","y","a"),("x","y","b"),("x","c","b")] []
t12 = matchn ("x","?y","?z")  [("x","y","a"),("x","y","b"),("x","c","b")] []

answer :: [Edge] -> [Edge] -> Ctx -> [Ctx]
answer (p:[]) vs m = matchn p vs m
answer (p:ps) vs m = 
 let ms = matchn p vs m in concat (map (answer ps vs) ms)

answer0 :: [Edge] -> [Edge] -> [Ctx]
answer0 ps vs = answer ps vs []

-- tests
t13 = answer0 [("?x","y","?z"),("?z","v","w")] [("b","v","w"),("a","y","b")]
t14 = answer0 [("?z","v","w"),("?x","y","?z")] [("b","v","w"),("a","y","b")]
t15 = answer0 [("?x","y","?z"),("?z","v","w")] [("b","v","w"),("a","y","b"),("c","y","b")]

--- 
eq :: [Edge] -> [Edge] -> Maybe Ctx
eq ps qs = if (length ps) /= (length qs) then failled
 else case answer0 ps qs of
            []    -> failled
            (r:_) -> succeed r

t16 = eq [("?x","a","b")] [("?y","a","b")]

get' :: Map [Edge] [Ctx] -> [Edge] -> Maybe (Ctx,[Ctx])
get' []        _ = Nothing
get' ((k,v):m) x = case (eq x k) of
 Nothing -> Nothing
 Just c  -> Just (c,v)

t17 = get' [([("?x","a","b")],[[("?x","c")]])] [("?y","a","b")]
-- Just ([("?y","?x")],[[("?x","c")]])

change :: Ctx -> [Ctx] -> [Ctx]
change c r = map (map (\(k,v) -> (eval1 c k,v))) r

t18 = change [("?y","?x")] [[("?x","c")]]

eval1 :: Ctx -> String -> String
eval1 c x = case get'' c x of
 Nothing -> x
 Just r  -> r 

--eval3 c (x,y,z) = (eval1 c x,eval1 c y,eval1 c z)
--evaln c xs = map (eval3 c) xs

answer0' :: Map [Edge] [Ctx] -> [Edge] -> [Edge] -> (Map [Edge] [Ctx], [Ctx])
answer0' m ps vs = case get' m ps of
 Nothing    -> let r=answer0 ps vs in (put m ps r,r)
 Just (c,r) -> (m,change c r)

---
slave :: (Read a, Show b) => (c -> a -> b) -> c -> PortNumber -> IO d
slave cmd mem port = withSocketsDo $ do 
 sock <- listenOn $ PortNumber port
 slavebody cmd mem sock
 
slavebody :: (Read a, Show a1) => (t -> a -> a1) -> t -> Socket -> IO b
slavebody cmd mem sock = forever $ do
 (handle, host, port) <- accept sock
 query <- hGetLine handle
 let x = read query
 let y = cmd mem x
 hPutStrLn handle (show y)
 hFlush handle
 hClose handle

--slave' :: (Read a, Show b) => (c -> a -> b) -> c -> PortNumber -> IO d
slave' cmd mem mem' port = withSocketsDo $ do 
 sock <- listenOn $ PortNumber port
 slavebody' cmd mem mem' sock
 
--slavebody' :: (Read a, Show a1) => (t -> a -> a1) -> t -> Socket -> IO b
slavebody' cmd mem mem' sock = do
 (handle, host, port) <- accept sock
 query <- hGetLine handle
 let x = read query
 let (mem2,y) = cmd mem' x mem
 hPutStrLn handle (show y)
 hFlush handle
 slavebody' cmd mem mem2 sock

client :: HostName -> PortNumber -> String -> IO String
client host port x = withSocketsDo $ do
 handle <- connectTo host (PortNumber port)
 hPutStrLn handle x
 hFlush handle
 y <- hGetLine handle
 hClose handle
 return y

---
query1 :: Action IO [Document]
query1 = rest =<< find(select ["l" =: "Sex","d" =: "male"] "edges")

prn ds = liftIO $ mapM_ (print . exclude ["_id"]) ds

mongo qry = do
 pipe <- connect (host "127.0.0.1")
 e <- access pipe master "test" (qry >>= prn)
 close pipe
 
populate = do
 f <- readFile "db.dat"
 let db = read f :: [Edge]
 let db'= insertMany "edges" (map (\(s,l,d) -> ["s" =: s, "l" =: l, "d" =: d]) db)
 pipe <- connect (host "127.0.0.1")
 e <- access pipe master "test" db'
 close pipe
 print e
---
type Map a b = [(a,b)]

put :: Map a b -> a -> b -> Map a b
put m k v = (k,v):m

get :: Eq a => Map a b -> a -> Maybe b
get []        _ = Nothing
get ((k,v):m) x = if (x==k) then Just v else get m x

get'' :: Eq b => Map a b -> b -> Maybe a
get'' []        _ = Nothing
get'' ((k,v):m) x = if (x==v) then Just k else get'' m x

