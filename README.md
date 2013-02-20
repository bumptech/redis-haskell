# redis-haskell

Haskell bindings for Redis, a fast persistent key-value store.

## Documentation

Hackage link to come.

## How to use this library

Unfortunately this package is not yet available from Hackage. Therefore to use this package, clone it and install it locally:

    $ git clone https://github.com/bumptech/redis-haskell.git
    $ cd redis-haskell
    $ cabal install
    
If Cabal complains of a missing `"Missing C library: uuid"`, then (on Ubuntu/Debian):

    $ sudo apt-get install uuid-dev

And finally update your project's `.cabal` file to include:

    ,redis-haskell >= 0.0.3.2