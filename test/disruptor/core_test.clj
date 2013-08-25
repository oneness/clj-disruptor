(ns disruptor.core_test
  (:use clojure.test)
  (:require [disruptor.core :as core]
            [disruptor.event :as event]
            [disruptor.events :as events])
  (:import [com.lmax.disruptor.dsl ProducerType]))

;; test helpers

(defn create-executor-service 
  "Simple Cached Thread Pool"
  []
  (java.util.concurrent.Executors/newCachedThreadPool))

(def default-ring-size (* 8 1024))
(def default-num-event-processors 4)
(def create-event (events/create-object-array-event-factory 5))

(defn timestamp []
  (System/currentTimeMillis))

;; # global state to track number of handles.  
;; This could be the application state or model    
;; depending on how the event handlers are configured,
;; this may not need th be thread safe
(def handle-count (atom 0))

(defn inc-handle-count []
  (swap! handle-count inc))

(defn reset-count [handle-count value]
  (reset! handle-count value))

;; # event handlers
;; note - return values are ignored unless the handler is registered with a 'slot'
;; (see below)

(defn handler-A [event sequence-num end-of-batch?]
  (inc-handle-count)
  {:A (timestamp)})

(defn handler-B [event sequence-num end-of-batch?]
  (inc-handle-count)
  {:B (timestamp)})

(defn handler-C [event sequence-num end-of-batch?]
  (inc-handle-count)
  {:C (timestamp)})

(defn handler-D [event sequence-num end-of-batch?]
  (inc-handle-count)
  {:D (timestamp)})

;; this is used to terminate the disruptor and print the final event  
;; in practice you would likely not do this.
(defn make-done-handler [iterations disruptor start-time]
  (fn [event sequence-num end-of-batch?]
    (inc-handle-count)
    (when (>= sequence-num iterations)
      (println "stopping")
      (.start (java.lang.Thread. #(core/shutdown disruptor))) 
      
      (let [hc @handle-count
            t (- (timestamp) start-time)
            handles-per-second (/ (* hc 1000.0) t)]
        (println)
        (println (events/pstr event))
        (println)
        (println (format "%d iterations, %d handles, handles/s: %.0f, time(s) %.2f" 
                           iterations
                           hc
                           handles-per-second
                           (/ t 1000.0)
                           ))))))

;; # General setup

(defn create-executor-service-p 
  "Tracks the number of threads created this was just for debugging"
  []
  (let [t-count (atom 0)
        thread-factory (proxy [java.util.concurrent.ThreadFactory] []
                         (newThread [^Runnable runnable] 
                                    (do
                                      (println "New thread: " (swap! t-count inc))
                                      (java.lang.Thread. runnable))))]
    (java.util.concurrent.Executors/newCachedThreadPool thread-factory)  
    ))

;; # run the example 

(import '[com.lmax.disruptor RingBuffer])
(defn publish-events 
  "publish a bunch of events to the ring buffer"
  [^RingBuffer ring-buffer iterations]
  (loop [i 0]
      (when-not (> i iterations)
        (core/publish ring-buffer 0 {:i i :start (timestamp)})
        (recur (unchecked-add 1 i))
        )))

(defn go 
  "run the example"
  ([iterations] (go iterations default-ring-size default-num-event-processors))
  ([iterations ring-size num-event-processors]
    (println "Started")
    (reset-count handle-count 0)
    (let [executor-service (create-executor-service)
          disruptor (core/make-disruptor create-event
                                         default-ring-size
                                         executor-service
                                         ProducerType/SINGLE
                                         :blocking)]
      (-> 
        (events/handle-events-with disruptor {1 handler-A})
        (events/then-handle {2 handler-B 3 handler-C})
        (events/then-handle {4 handler-D})
        (events/then-handle (make-done-handler iterations disruptor (timestamp)))
        ) 
      (let [ring-buffer (core/start disruptor)]  
        (.start (java.lang.Thread. #(publish-events ring-buffer iterations)))))))

(comment
  (def iterations 300000000)
  
  (def iterations 300000)
  (def iterations 10)
  (go iterations)
  )

    
;; tests
(deftest make-disruptor
  (testing "Should be able to make disruptor"
    (is (core/make-disruptor
         create-event
         default-ring-size
         (create-executor-service)
         ProducerType/SINGLE
         :blocking))))
