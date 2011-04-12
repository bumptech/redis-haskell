{-# LANGUAGE DeriveDataTypeable, FlexibleContexts #-}

module Database.Redis.Core where

import Control.Monad.Trans        ( MonadIO, liftIO )
import Control.Exception          ( Exception(..), finally )
import Data.Typeable              ( Typeable )
import Network                    ( HostName, PortNumber, withSocketsDo)
import Network.Socket             ( getAddrInfo, defaultHints, addrFamily, 
                                    Socket, socket, sClose, defaultProtocol,
                                    addrAddress, defaultProtocol, SocketType(Stream),
                                    AddrInfo(..), AddrInfoFlag(..), Family(..))
import Control.Failure            ( Failure, wrapFailure )
import qualified Network.Socket
                                   
import qualified Data.ByteString.Lazy.Char8 as B
import qualified Data.ByteString.Char8 as S

-- ---------------------------------------------------------------------------
-- Types
-- 

-- | Provides a wrapper for a handle to a Redis server.
newtype Server = Server { redisSocket :: Socket}

-- | Provides a type for a key parameter for Redis commands.
type RedisKey = B.ByteString

-- | Provides a type for a value parameter for Redis commands, where a value 
-- parameter is any parameter that isn't a key.
type RedisParam = B.ByteString

-- | Data type representing a return value from a Redis command.
data RedisValue = RedisString S.ByteString
                | RedisInteger Int
                | RedisMulti [RedisValue] 
                | RedisNil
                  deriving (Eq, Show)

-- ---------------------------------------------------------------------------
-- Failure
-- 

data RedisError = ServerError String
                | OperationTimeout
                  deriving (Show, Typeable)

instance Exception RedisError

-- This is kinda lame, but System.Timeout does not export its Timeout exception
-- to match on it
errorWrap :: IO a -> IO a
errorWrap f = wrapFailure (\e -> if (show e == "<<timeout>>") then OperationTimeout else (ServerError $ show e)) f

-- ---------------------------------------------------------------------------
-- Connection
-- 

-- | Establishes a connection with the Redis server at the given host and 
-- port.
connect :: (MonadIO m, Failure RedisError m) => HostName -> PortNumber -> m Server
connect host port =
    liftIO $ errorWrap (
        withSocketsDo $ do
            addrinfos <- getAddrInfo
                         (Just defaultHints {
                                addrFlags = [AI_PASSIVE],
                                addrFamily = AF_INET
                                }) 
                         (Just host)
                         (Just $ show port)
            let addr = head addrinfos
            sock <- socket (addrFamily addr) Stream defaultProtocol
            Network.Socket.connect sock $ addrAddress addr
            return $ Server sock
        )

-- | Disconnects a server handle.
disconnect :: Server -> IO ()
disconnect = sClose . redisSocket

-- | Runs some action with a connection to the Redis server at the given host 
-- and port
withRedisConn :: HostName -> PortNumber -> (Server -> IO ()) -> IO ()
withRedisConn host port action = do
    r <- connect host port
    action r `finally` disconnect r

-- | Runs some action with a connection to the Redis server operation at 
-- localhost on port 6379 (this is the default Redis port).
withRedisConn' :: (Server -> IO ()) -> IO ()
withRedisConn' = withRedisConn "localhost" 6379

toParam :: Show a => a -> B.ByteString
toParam = B.pack . show
