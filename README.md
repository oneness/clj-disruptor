# clj-disruptor

clojure wrapper for the LMax Disruptor.

references :

* http://martinfowler.com/articles/lmax.html
* http://www.infoq.com/presentations/LMAX

## Usage

* clojars [oneness/clj-disruptor "0.0.2"]

```clojure
From within clj-disruptor folder run lein repl then type:
(require '[disruptor.core_test :test] :reload)

;; pubish 1000 events
(test/go 1000)

```
## TODO
* more tests
* more exposure of LMAX dsl package


## License

Copyright (C) 2012 Dave Sann, Kasim Tuman

Distributed under the Eclipse Public License, the same as Clojure.
