{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ExtendedDefaultRules #-}

module Mongoo where

import DB
import Data.Time (getCurrentTime,diffUTCTime)
import Control.Exception.Base (evaluate)

import Database.MongoDB
import Control.Monad.Trans (liftIO)

populate = do
 f <- readFile "titanic.dhs"
 let db = read f :: Graph
 let db'= insertMany "edges" (map (\(s,l,d) -> ["s" =: s, "l" =: l, "d" =: d]) db)
 pipe <- connect (host "127.0.0.1")
 e <- access pipe master "test" db'
 close pipe
 print e

query1,query2,query3 :: Action IO [Document]
query1 = rest =<< find(select ["l" =: "Sex","d" =: "male"] "edges")
query2 = rest =<< find(select ["l" =: "Sex","d" =: "male"] "edges")
query3 = rest =<< find(select [] "edges")

prn ds = liftIO $ mapM_ (print . exclude ["_id"]) ds

run q qry = do
 pipe <- connect (host "127.0.0.1")
 t1 <- getCurrentTime
 e <- access pipe master "test" qry -- (qry >>= prn)
 evaluate e
 t2 <- getCurrentTime
 putStrLn (q ++ " in " ++ (show (diffUTCTime t2 t1)))
 close pipe

