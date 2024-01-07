(ns otus-31.flamegraphs
  (:require [clj-async-profiler.core :as prof]))

;; Profile the following expression:
(prof/profile (dotimes [i 10000] (reduce + (range i))))

;; The resulting flamegraph will be stored in /tmp/clj-async-profiler/results/
;; You can view the HTML file directly from there or start a local web UI:


(defn burn-cpu [op secs]
  (let [start (System/nanoTime)]
    (while (< (/ (- (System/nanoTime) start) 1e9) secs)
      (op))))

(defn test-one []
  (burn-cpu #(reduce + (map inc (range 1000))) 10))

(prof/profile (test-one))

(prof/serve-ui 8080) ; Serve on port 8080
