{-# LANGUAGE DeriveDataTypeable, FlexibleContexts, ScopedTypeVariables #-}

module Database.Redis.SubHub where

import qualified Data.Map as M
import Data.Map                   ( (!) )
import Control.Failure            ( failure, Failure )
import Control.Exception          ( try, bracket )
import Control.Concurrent         ( forkIO, threadDelay )
import Control.Monad              ( when )
import Control.Concurrent.STM     (atomically)
import Control.Concurrent.STM.TVar (TVar, readTVarIO, newTVarIO, writeTVar)
import Control.Concurrent.STM.TChan    (TChan, newTChanIO, writeTChan, readTChan, isEmptyTChan)
import Network                    ( HostName, PortNumber )
import System.IO                  ( hPutStrLn, stderr )
import qualified Data.Time.Format as DTF
import Data.Time.Clock (UTCTime, getCurrentTime)
import System.Locale (defaultTimeLocale)
import qualified Data.ByteString.Lazy.Char8 as B
import qualified Data.ByteString.Char8 as S
import System.UUID.V4 (uuid)
import Data.UUID (UUID)

import Database.Redis.Core (withRedisConn, RedisKey,
                            RedisValue(..), Server, RedisError(..),
                            errorWrap )
import Database.Redis.Internal ( getReply, multiBulk )

type SubHubQueue = TChan (RedisKey, RedisValue)
type SubHubState = (TChan (Bool, UUID, RedisKey, SubHubQueue), TVar Bool)
type SubMap = M.Map RedisKey [(UUID, SubHubQueue)]
data SubHub = SubHub HostName PortNumber SubHubState
type SubTicket = (UUID, [RedisKey], SubHubQueue)

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
    inpChan <- newTChanIO
    running <- newTVarIO True
    let state = (inpChan, running)
    let sh = SubHub host port state
    tm <- newTVarIO M.empty
    _ <- forkIO $ hubloop sh tm
    return $ sh

hubloop :: SubHub -> TVar SubMap -> IO ()
hubloop sh@(SubHub host port state) tm = do
    r <- try $ withRedisConn host port $ (\conn -> reconnect tm conn >> (return =<< connectedLoop tm conn ""))
    case r of
        Right _ -> return ()
        Left (ServerError e) -> logError $ "{subhub} connection error (" ++ host ++ ":" ++ (show port) ++ ") " ++ e
        Left _ -> fail "{subhub} system error"
    let (_, vrunning) = state
    running <- readTVarIO  vrunning
    when running $ (logReconnect >> threadDelay 200000 >> hubloop sh tm) -- loop (and reconnect)
  where
    reconnect :: TVar SubMap -> Server -> IO ()
    reconnect tm' conn = do
        submap <- readTVarIO tm'
        mapM_ (issueSubCommand conn) (M.keys submap)

    connectedLoop :: TVar SubMap -> Server -> S.ByteString -> IO ()
    connectedLoop tm' conn buf = do
        submap <- readTVarIO tm'
        mr <- try $ errorWrap $ getReply conn buf Nothing (Just 100000)
        buf' <- case mr of
            Left OperationTimeout -> return buf
            Left e -> failure e
            Right (v, b) -> dispatch submap v >> return b

        let (newsubs, vrunning) = state
        submap' <- addSubs conn newsubs submap
        atomically $ writeTVar tm submap'
        running <- readTVarIO vrunning
        when running $ connectedLoop tm conn buf'

    logReconnect = logError "{subhub} disconnect/reconnect to redis..."

    addSubs s newsubs submap = do
        emp <- atomically $ isEmptyTChan newsubs
        if emp
            then (return submap)
            else (do
                (add, cid, nkey, chan) <- atomically $ readTChan newsubs
                if add then (do
                    if (not $ M.member nkey submap)
                        then issueSubCommand s nkey
                        else return ()
                    let submap' = M.insertWith' (++) nkey [(cid, chan)] submap
                    addSubs s newsubs submap'
                    )
                else (do
                    let submap' = M.update (\l -> Just $ filter (\(i,_)-> i/=cid) l) nkey submap
                    let empty = ((M.member nkey submap') && (length (submap' ! nkey) == 0))
                    when empty $ issueUnSubCommand s nkey
                    let submap'' = if empty then M.delete nkey submap' else submap'
                    addSubs s newsubs submap''
                    )
                )

issueSubCommand :: Server -> RedisKey -> IO ()
issueSubCommand s k = errorWrap $ multiBulk s "SUBSCRIBE" [k]

issueUnSubCommand :: Server -> RedisKey -> IO ()
issueUnSubCommand s k = errorWrap $ multiBulk s "UNSUBSCRIBE" [k]

dispatch :: SubMap -> RedisValue -> IO ()
dispatch m (RedisMulti [RedisString "message", RedisString key, RedisString msg]) = do
    let keyl = B.fromChunks [key]
    let chans = map snd $ M.findWithDefault [] keyl m
    mapM_ (deliver keyl) chans
  where
    deliver k c = atomically $ writeTChan c (k, RedisString msg)

dispatch _ _ = return () -- ignoring subscribe, unsubscribe

sub :: SubHub -> [RedisKey] -> IO SubTicket
sub (SubHub _ _ (inq, _)) keys = do
    c <- newTChanIO
    cid <- uuid
    mapM_ (makeSubRequest c cid) keys
    return (cid, keys, c)
  where
    makeSubRequest c' cid' k = atomically $ writeTChan inq (True, cid', k, c')

getsub :: SubTicket -> IO (RedisKey, RedisValue)
getsub (_, _, c) = atomically $ readTChan c

unsub :: SubHub -> SubTicket -> IO ()
unsub (SubHub _ _ (inq, _)) (cid, keys, c) = do
    mapM_ (makeUnSubRequest c cid) keys
  where
    makeUnSubRequest c' cid' k = atomically $ writeTChan inq (False, cid', k, c')

withsub :: SubHub -> [RedisKey] -> (SubTicket -> IO a) -> IO a
withsub s keys a =
    bracket (sub s keys) (unsub s) a

destroySubHub :: SubHub -> IO ()
destroySubHub (SubHub _ _ (_, trunning)) = do
    atomically $ writeTVar trunning False

{-
 -
 - Model:
 -
 - Start, just sub and it will get picked up within 1/10s or sooner (timeout on block from hub)
 -}
