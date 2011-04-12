{-# LANGUAGE FlexibleContexts, OverloadedStrings #-}

module Database.Redis.Internal where

import Control.Monad.Trans        ( MonadIO, liftIO )
import Control.Failure            ( Failure, failure )
import Control.Monad              ( when )
import Database.Redis.Core
import qualified Data.ByteString.Lazy.Char8 as B
import qualified Data.ByteString.Char8 as S
import Network.Socket.ByteString (recv, sendAll)
import Network.Socket            ( fdSocket )
import Data.Binary.Put (runPut, Put, putLazyByteString)
import Data.Attoparsec (Parser, parse, Result(..), takeTill, string)
import qualified Data.Attoparsec as Atto
import System.Timeout             ( timeout )
import Control.Concurrent         ( threadWaitRead )
import System.Posix.Types         ( Fd(..) )


-- ---------------------------------------------------------------------------
-- Command
-- 
--

command :: (MonadIO m, Failure RedisError m) => Server -> IO () -> m RedisValue
command r f = liftIO $ errorWrap (f >> getSimpleReply r)

multiBulk :: Server -> B.ByteString -> [B.ByteString] -> IO ()
multiBulk (Server s) command' vs = do
    let output = runPut $ formatRedisRequest $ command' : vs
    liftIO $ sendAll s $ (S.concat . B.toChunks) output
    return ()

multiBulkT2 :: Server -> B.ByteString -> [(B.ByteString, B.ByteString)] -> IO ()
multiBulkT2 r command' kvs = do
    multiBulk r command' $ concatMap (\kv -> [fst kv] ++ [snd kv]) kvs

eol :: B.ByteString
eol = "\r\n"

seol :: S.ByteString
seol = "\r\n"

formatRedisRequest :: [B.ByteString] -> Put 
formatRedisRequest allVs = do
    putArgCount  allVs
    putArgs      allVs
  where
    putArgCount :: [B.ByteString] -> Put
    putArgCount xs = mapM_ putLazyByteString ["*", toParam $ length xs, eol]

    putArgs :: [B.ByteString] -> Put
    putArgs xs = mapM_ putArg xs

    putArg :: B.ByteString -> Put
    putArg x = mapM_ putLazyByteString ["$", toParam $ B.length x, eol, x, eol]

-- ---------------------------------------------------------------------------
-- Reply, using attoparsec
--

getSimpleReply :: Server -> IO RedisValue
getSimpleReply s = do
    (r, buf) <- getReply s "" Nothing Nothing
    when (S.length buf > 0) $ failure $ ServerError "getSimpleReply should _never_ have buffer remaining"
    return r

getReply :: Server -> S.ByteString -> Maybe (S.ByteString -> Result RedisValue) -> Maybe Int -> IO (RedisValue, S.ByteString)
getReply r i Nothing to = case parse parseReply i of
    Done remaining result -> return (result, remaining)
    Partial continueParse -> getReply r "" (Just continueParse) to
    _ -> fail "unexpected result from parser"

getReply r@(Server h) "" (Just continueParse) mto = do
    buf <- case mto of
        Just to -> do
            mreadable <- timeout to $ threadWaitRead $ Fd (fdSocket h)
            case mreadable of
                Just () -> recv h 8096
                Nothing -> failure OperationTimeout
        Nothing -> recv h 8096
    
    case (S.length buf) of
        0 -> error "connection closed by remote redis server"
        _ -> case (continueParse buf) of
                Done remaining result -> return (result, remaining)
                Partial continueParse' -> getReply r "" (Just continueParse') mto
                Fail _ _ msg -> error $ "attoparsec:" ++ msg

parseReply :: Parser RedisValue
parseReply = do
    prefix <- Atto.take 1
    case prefix of
        ":" -> integerReply
        "$" -> bulkReply
        "+" -> singleLineReply
        "-" -> errorReply >> return RedisNil 
        "*" -> multiBulkReply
        _ -> error "redis protocol error: unknown reply type!"

singleLineReply :: Parser RedisValue
singleLineReply = readLineContents >>= \s-> return $ RedisString s

errorReply :: Parser ()
errorReply = readLineContents >>= \s -> error $ "redis daemon error: " ++ (S.unpack s)


bulkReply :: Parser RedisValue
bulkReply = do
    i <- readIntLine
    s <- Atto.take i
    _ <- string seol
    return $ RedisString s

multiBulkReply :: Parser RedisValue
multiBulkReply = do
    numParams <- readIntLine
    args <- mapM (\_ -> parseReply) [1..numParams]
    return $ RedisMulti args

integerReply :: Parser RedisValue
integerReply = do
    i <- readIntLine
    return $ RedisInteger i

readIntLine :: Parser Int
readIntLine = do
    line <- readLineContents
    return $ read $ S.unpack line

readLineContents :: Parser S.ByteString
readLineContents = do
    v <- takeTill (==13)
    _ <- string seol
    return v


boolify :: (MonadIO m, Failure RedisError m) => m RedisValue -> m Bool
boolify v = do
    v' <- v
    return $ case v' of RedisString "OK" -> True
                        RedisInteger 1   -> True
                        _                -> False

discard :: (Monad a) => a b -> a ()
discard f = f >> return ()

