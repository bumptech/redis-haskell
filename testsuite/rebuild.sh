find cabal-dev/ -name "redis-haskell*" | xargs rm -rf 
cabal-dev add-source ../
cabal-dev install-deps && ./runtest.sh
