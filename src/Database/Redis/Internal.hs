{-# LANGUAGE FlexibleContexts, OverloadedStrings #-}

module Database.Redis.Internal where

import Control.Monad.Trans        ( MonadIO, liftIO )
import Control.Failure            ( Failure )
import Database.Redis.Core
import qualified Data.ByteString.Lazy.Char8 as B
import qualified Data.ByteString.Char8 as S
import Network.Socket.ByteString (recv, sendAll)
import Data.Binary.Put (runPut, Put, putLazyByteString)
import Data.Attoparsec (Parser, parse, Result(..), takeTill, string)
import qualified Data.Attoparsec as Atto

-- ---------------------------------------------------------------------------
-- Command
-- 
--

command :: (MonadIO m, Failure RedisError m) => Server -> IO () -> m RedisValue
command r f = liftIO $ errorWrap (f >> getReply r Nothing)

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

getReply :: Server -> Maybe (S.ByteString -> Result RedisValue) -> IO RedisValue
getReply r Nothing = case parse parseReply "" of
    Partial continueParse -> getReply r (Just continueParse)
    _ -> fail "unexpected result from parser"

getReply r@(Server h) (Just continueParse) = do
    buf <- liftIO $ recv h 8096
    {- TODO: handle length == 0 -}
    case (continueParse buf) of 
        Done _ result -> return result
        Partial continueParse' -> getReply r (Just continueParse') 
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
    args <- mapM (\_-> string "$" >> bulkReply) [1..numParams]
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

