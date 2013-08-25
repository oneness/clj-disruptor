(ns disruptor.core
  "create, start, shutdown, disruptors"
  (:require [disruptor.event :as de])
  (:import
   [com.lmax.disruptor.dsl Disruptor]
   [com.lmax.disruptor
     EventFactory
     EventHandler
     BlockingWaitStrategy
     BusySpinWaitStrategy
     SleepingWaitStrategy
     YieldingWaitStrategy
     RingBuffer]))

(set! *warn-on-reflection* true)

(defn make-event-factory [create-event]
  (reify EventFactory
    (newInstance [this] (create-event))))

(defn make-wait-strategy [strategy]
  (case strategy
    :blocking  (BlockingWaitStrategy.)
    :busy-spin (BusySpinWaitStrategy.)
    :sleeping  (SleepingWaitStrategy.)
    :yielding  (YieldingWaitStrategy.)))

(defn make-disruptor
  "Make a disruptor "
  ([create-event-fn ring-size executor-service producer-type wait-strategy]
    (Disruptor.
     (make-event-factory create-event-fn)
     ring-size
     executor-service
     producer-type
     (make-wait-strategy  wait-strategy))))

(defn start
  "Start the disruptor - returns the ring buffer"
  ^RingBuffer [^Disruptor disruptor]
  (.start disruptor))

(defn shutdown
  "shutdown the disruptor
   - will shutdown at a clean point - not in the middle of an event."
  [^Disruptor disruptor]
  (.shutdown disruptor))

(defn get-event
  "get the event at the given sequence-num"
  [^RingBuffer ring-buffer sequence-num]
  (.get ring-buffer sequence-num))

(defn claim-next
   "claim the next sequence-num for publishing"
  ^long [^RingBuffer ring-buffer]
  (.next ring-buffer))

(defn publish
  "publish data to the ring buffer in slot-key of the next available event"
  [^RingBuffer ring-buffer slot-key data]
  (let [sequence-num (claim-next ring-buffer)
        event (get-event ring-buffer sequence-num)]
    (de/set-slot! event slot-key data)
    (.publish ring-buffer sequence-num)))

(defn get-cursor [^RingBuffer ring-buffer]
  (.getCursor ring-buffer))
