{-# LANGUAGE DeriveDataTypeable, FlexibleContexts, ScopedTypeVariables #-}

module Database.Redis.SubHub where

import qualified Data.Map as M
import Control.Monad.Trans        ( MonadIO, liftIO )
import Control.Failure            ( failure, Failure )
import Control.Exception          ( Exception(..), SomeException, try, finally )
import Control.Concurrent         ( forkIO, threadDelay )
import Control.Monad              ( when )
import Control.Concurrent.STM     (atomically)
import Control.Concurrent.STM.TVar (TVar, readTVarIO, newTVarIO, readTVar, writeTVar)
import Control.Concurrent.Chan    (Chan, newChan, writeChan, readChan, isEmptyChan)
import Network                    ( HostName, PortNumber )
import System.Timeout             ( timeout )
import System.IO                  ( hPutStrLn, stderr )
import qualified Data.Time.Format as DTF
import Data.Time.Clock (UTCTime, getCurrentTime)
import System.Locale (defaultTimeLocale)
import qualified Data.ByteString.Lazy.Char8 as B
import Data.Maybe (fromJust)

import Database.Redis.Core (withRedisConn, RedisKey,
                            RedisValue(..), Server, RedisError(..),
                            errorWrap )
import Database.Redis.Internal ( getReply, multiBulk )

import Debug.Trace (trace)

type SubHubQueue = Chan (RedisKey, RedisValue)
type SubHubState = (Chan (RedisKey, SubHubQueue), TVar Bool)
type SubMap = M.Map RedisKey [SubHubQueue]
data SubHub = SubHub HostName PortNumber SubHubState

standardFormat :: String
standardFormat = "%Y/%m/%d %H:%M:%S"

standardFormatTime :: UTCTime -> String
standardFormatTime = DTF.formatTime defaultTimeLocale standardFormat

logError :: String -> IO ()
logError s = do
    tm <- getCurrentTime
    let d = standardFormatTime tm
    hPutStrLn stderr $ d ++ " " ++ s

createSubHub :: HostName -> PortNumber -> IO SubHub
createSubHub host port = do
    inpChan <- newChan
    running <- newTVarIO True
    let state = (inpChan, running)
    let sh = SubHub host port state
    tm <- newTVarIO M.empty
    _ <- forkIO $ hubloop sh tm
    return $ sh

hubloop :: SubHub -> TVar SubMap -> IO ()
hubloop sh@(SubHub host port state) tm = do
    r <- try $ withRedisConn host port $ connectedLoop tm
    case r of
        Right _ -> return ()
        Left (ServerError e) -> logError $ "{subhub} connection error (" ++ host ++ ":" ++ (show port) ++ ") " ++ e
    let (_, vrunning) = state
    running <- readTVarIO  vrunning
    when running $ (logReconnect >> threadDelay 200000 >> hubloop sh tm) -- loop (and reconnect)
  where
    connectedLoop :: TVar SubMap -> Server -> IO ()
    connectedLoop tm conn = do
        submap <- readTVarIO tm
        mr <- try $ timeout 100000 $ errorWrap $ getReply conn Nothing
        case mr of
            Left (OperationTimeout) -> print "timeout"
            Left (e) -> failure e
            Right mv -> dispatch submap $ fromJust mv

        let (newsubs, vrunning) = state
        submap' <- addSubs conn newsubs submap
        atomically $ writeTVar tm submap'
        running <- readTVarIO vrunning
        when running $ connectedLoop tm conn

    logReconnect = logError "{subhub} disconnect/reconnect to redis..."

    addSubs s newsubs submap = do
        emp <- isEmptyChan newsubs
        if emp
            then (return submap)
            else (do
                (nkey, chan) <- readChan newsubs
                if (not $ M.member nkey submap)
                    then issueSubCommand s nkey
                    else return ()
                let submap' = M.insertWith' (++) nkey [chan] submap
                addSubs s newsubs submap'
                )

issueSubCommand :: Server -> RedisKey -> IO ()
issueSubCommand s k = errorWrap $ multiBulk s "SUBSCRIBE" [k]

dispatch :: SubMap -> RedisValue -> IO ()
dispatch m (RedisMulti [RedisString "message", RedisString key, RedisString msg]) = do
    let keyl = B.fromChunks [key]
    let chans = M.findWithDefault [] keyl m
    mapM_ (deliver keyl) chans
  where
    deliver key c = writeChan c (key, RedisString msg)

dispatch _ _ = return () -- ignoring subscribe, unsubscribe

sub :: SubHub -> [RedisKey] -> IO SubHubQueue
sub (SubHub _ _ (inq, _)) keys = do
    c <- newChan
    mapM_ (makeSubRequest c) keys
    return c
  where
    makeSubRequest c k = writeChan inq (k, c)

getsub :: SubHubQueue -> IO (RedisKey, RedisValue)
getsub c = readChan c

destroySubHub :: SubHub -> IO ()
destroySubHub = undefined


{-
 -
 - Model:
 -
 - Start, just sub and it will get picked up within 1/10s or sooner (timeout on block from hub)
 -}
